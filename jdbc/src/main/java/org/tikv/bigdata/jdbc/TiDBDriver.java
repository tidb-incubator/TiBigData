/*
 * Copyright 2020 TiKV Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tikv.bigdata.jdbc;

import static java.lang.String.format;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.LoggerFactory;

/**
 * jdbc:tidb://host:port/database
 */
public class TiDBDriver extends LoadBalancingDriver {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TiDBDriver.class);

  /**
   * disable cache if value <= 0
   */
  public static final String TIDB_URLS_CACHE_MS = "tidb.urls-cache-ms";

  public static final String TIDB_URLS_CACHE_MS_DEFAULT = "0";

  public static final String MYSQL_PREFIX = "jdbc:mysql://";

  public static final String TIDB_PREFIX = "jdbc:tidb://";

  public static final String MYSQL_URL_PREFIX_REGEX = "jdbc:mysql://[^/]+:\\d+";

  public static final String MYSQL_DRIVER_NAME = "com.mysql.jdbc.Driver";

  public static final String QUERY_TIDB_SERVER_SQL =
      "SELECT `IP`,`PORT` FROM `INFORMATION_SCHEMA`.`TIDB_SERVERS_INFO` ";

  static {
    try {
      java.sql.DriverManager.registerDriver(new TiDBDriver());
    } catch (SQLException e) {
      throw new RuntimeException("Can't register driver!", e);
    }
  }

  private Map<String, Collection<String>> urlsCache = new ConcurrentHashMap<>();

  private Map<String, Long> urlsTime = new ConcurrentHashMap<>();

  public TiDBDriver() throws SQLException {
    super(MYSQL_DRIVER_NAME);
  }

  @Override
  public Connection connect(String tidbUrl, Properties info) throws SQLException {
    return connect(tidbUrl, info, true);
  }

  public Connection connect(String tidbUrl, Properties info, boolean retry) throws SQLException {
    Properties properties = parseProperties(tidbUrl, info);
    long ttl = Long
        .parseLong(properties.getProperty(TIDB_URLS_CACHE_MS, TIDB_URLS_CACHE_MS_DEFAULT));
    if (ttl <= 0) {
      return super.connect(String.join(",", queryAllUrls(tidbUrl, properties)), properties);
    }
    long currentTime = System.currentTimeMillis();
    synchronized (tidbUrl) {
      if (currentTime - urlsTime.getOrDefault(tidbUrl, 0L) > ttl) {
        LOG.debug("cache is not available, update cache");
        urlsCache.put(tidbUrl, queryAllUrls(tidbUrl, properties));
        urlsTime.put(tidbUrl, currentTime);
      } else {
        LOG.debug("cache is available, use cache");
      }
      Collection<String> cacheUrls = urlsCache.get(tidbUrl);
      for (String url : cacheUrls) {
        LOG.debug("try connect to " + url);
        try {
          return driver.connect(url, properties);
        } catch (Exception e) {
          LOG.warn("connect to " + url + " fail, retry other url", e);
          // if exception occurs, we should update cache
          urlsTime.put(tidbUrl, 0L);
        }
      }
      if (retry) {
        return connect(tidbUrl, info, false);
      }
      throw new SQLException("can not get connection for cache urls: " + cacheUrls);
    }
  }

  @Override
  public boolean acceptsURL(String tidbUrl) throws SQLException {
    return super.acceptsURL(getMySqlUrl(tidbUrl));
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String tidbUrl, Properties info) throws SQLException {
    return driver.getPropertyInfo(getMySqlUrl(tidbUrl), info);
  }

  private Collection<String> queryAllUrls(String tidbUrl, Properties info)
      throws SQLException {
    String mySqlUrl = getMySqlUrl(tidbUrl);
    try (
        Connection connection = driver.connect(mySqlUrl, info);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(QUERY_TIDB_SERVER_SQL)) {
      List<String> list = new ArrayList<>();
      while (resultSet.next()) {
        String ip = resultSet.getString("IP");
        int port = resultSet.getInt("PORT");
        list.add(
            mySqlUrl.replaceFirst(MYSQL_URL_PREFIX_REGEX, format("jdbc:mysql://%s:%d", ip, port)));
      }
      Collection<String> urls = getUrlProvider(
          info.getProperty(URL_PROVIDER, DefaultUrlProvider.class.getName())).apply(list);
      if (LOG.isDebugEnabled()) {
        LOG.debug(format("query urls: %s, real urls: %s", list, urls));
      }
      return urls;
    }
  }

  private String getMySqlUrl(String tidbUrl) {
    return tidbUrl.replaceFirst(TIDB_PREFIX, MYSQL_PREFIX);
  }

  protected Properties parseProperties(String url, Properties info) {
    try {
      URI uri = new URI(url.substring("jdbc:".length()));
      Properties properties = new Properties();
      for (String kv : uri.getQuery().split("&")) {
        String[] split = kv.split("=");
        if (split.length == 2) {
          properties.put(split[0], split[1]);
        }
      }
      properties.putAll(info);
      return properties;
    } catch (Exception e) {
      throw new IllegalArgumentException("can not parse properties from url", e);
    }
  }
}
