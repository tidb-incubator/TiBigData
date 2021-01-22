/*
 * Copyright 2020 org.tikv.
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

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import org.slf4j.LoggerFactory;

/**
 * jdbc:tidb://host:port/database
 */
public class TiDBDriver extends LoadBalancingDriver {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TiDBDriver.class);

  public static final String MYSQL_PREFIX = "jdbc:mysql://";

  public static final String TIDB_PREFIX = "jdbc:tidb://";

  public static final String MYSQL_URL_PREFIX_REGEX = "jdbc:mysql://[^/]+:\\d+";

  public static final String MYSQL_DRIVER_NAME = "com.mysql.jdbc.Driver";

  public static final String QUERY_TIDB_SERVER_SQL =
      "SELECT `IP`,`PORT` FROM `INFORMATION_SCHEMA`.`TIDB_SERVERS_INFO` ";

  /**
   * The value is set by {@link System#setProperty(String, String)}
   */
  public static final String URL_PROVIDER = "url.provider";

  static {
    try {
      java.sql.DriverManager.registerDriver(new TiDBDriver());
    } catch (SQLException e) {
      throw new RuntimeException("Can't register driver!");
    }
  }

  public TiDBDriver() throws SQLException {
    super(MYSQL_DRIVER_NAME, createUrlProvider());
  }

  @Override
  public Connection connect(String tidbUrl, Properties info) throws SQLException {
    Collection<String> urls = queryAllUrls(tidbUrl, info);
    return super.connect(String.join(",", urls), info);
  }

  @Override
  public boolean acceptsURL(String tidbUrl) throws SQLException {
    return super.acceptsURL(getMySqlUrl(tidbUrl));
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String tidbUrl, Properties info) throws SQLException {
    return driver.getPropertyInfo(getMySqlUrl(tidbUrl), info);
  }

  @SuppressWarnings("unchecked")
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
      Collection<String> urls = urlProvider.apply(list);
      if (LOG.isDebugEnabled()) {
        LOG.debug(format("query urls: %s, real urls: %s", list, urls));
      }
      return urls;
    }
  }

  private String getMySqlUrl(String tidbUrl) {
    return tidbUrl.replaceFirst(TIDB_PREFIX, MYSQL_PREFIX);
  }

  @SuppressWarnings("unchecked")
  private static Function<Collection<String>, Collection<String>> createUrlProvider() {
    try {
      return (Function<Collection<String>, Collection<String>>) Class
          .forName(System.getProperty(URL_PROVIDER, DefaultUrlProvider.class.getName()))
          .newInstance();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

}
