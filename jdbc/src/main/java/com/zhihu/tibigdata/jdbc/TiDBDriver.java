/*
 * Copyright 2020 Zhihu.
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

package com.zhihu.tibigdata.jdbc;

import static java.lang.String.format;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.slf4j.LoggerFactory;

/**
 * jdbc:tidb://host:port/database
 */
public class TiDBDriver extends LoadBalanceDriver {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TiDBDriver.class);

  public static final String MYSQL_PREFIX = "jdbc:mysql://";

  public static final String TIDB_PREFIX = "jdbc:tidb://";

  public static final String MYSQL_URL_PREFIX_REGEX = "jdbc:mysql://[^/]+:\\d+";

  public static final String MYSQL_DRIVER_NAME = "com.mysql.jdbc.Driver";

  public static final String QUERY_TIDB_SERVER_SQL =
      "SELECT `IP`,`PORT` FROM `INFORMATION_SCHEMA`.`TIDB_SERVERS_INFO` ";

  public static final String TIDB_HOST_PORT_MAPPING_IMPL =
      "tidb.host-port-mapping.impl";

  public static final String TIDB_HOST_PORT_MAPPING_IMPL_DEFAULT =
      "com.zhihu.tibigdata.jdbc.HostPortMappingDefaultImpl";

  static {
    System.setProperty(BALANCE_DRIVER_NAME, MYSQL_DRIVER_NAME);
    try {
      java.sql.DriverManager.registerDriver(new TiDBDriver());
    } catch (SQLException e) {
      throw new RuntimeException("Can't register driver!");
    }
  }

  public TiDBDriver() throws SQLException {
  }

  @Override
  public Connection connect(String tidbUrl, Properties info) throws SQLException {
    String mysqlUrl = getMySqlUrl(tidbUrl);
    List<String> urls = queryTiDBServer(mysqlUrl, info).stream()
        .map(hostPort -> mysqlUrl
            .replaceFirst(MYSQL_URL_PREFIX_REGEX, MYSQL_PREFIX + hostPort.toString()))
        .collect(Collectors.toList());
    return super.connect(String.join(",", urls), info);
  }

  @Override
  public boolean acceptsURL(String tidbUrl) throws SQLException {
    return super.acceptsURL(getMySqlUrl(tidbUrl));
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String tidbUrl, Properties info) throws SQLException {
    return super.getPropertyInfo(getMySqlUrl(tidbUrl), info);
  }

  private List<HostPort> queryTiDBServer(String tidbUrl, Properties info)
      throws SQLException {
    String className = System
        .getProperty(TIDB_HOST_PORT_MAPPING_IMPL, TIDB_HOST_PORT_MAPPING_IMPL_DEFAULT);
    HostPortMapping hostPortMapping;
    try {
      hostPortMapping = (HostPortMapping) Class.forName(className).newInstance();
    } catch (Exception e) {
      throw new SQLException(e);
    }
    try (Connection connection = super.connect(getMySqlUrl(tidbUrl), info);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(QUERY_TIDB_SERVER_SQL)) {
      List<HostPort> list = new ArrayList<>();
      while (resultSet.next()) {
        list.add(new HostPort(resultSet.getString("IP"), resultSet.getInt("PORT")));
      }
      LOG.debug("query result of servers: " + list);
      List<HostPort> hostPortList = hostPortMapping.map(list);
      LOG.debug("mapping result of servers: " + hostPortList);
      return hostPortList;
    }
  }

  private String getMySqlUrl(String tidbUrl) {
    return tidbUrl.replaceFirst(TIDB_PREFIX, MYSQL_PREFIX);
  }

  public static class HostPort {

    private final String host;

    private final int port;

    public HostPort(String host, int port) {
      this.host = host;
      this.port = port;
    }

    public String getHost() {
      return host;
    }

    public int getPort() {
      return port;
    }

    @Override
    public String toString() {
      return format("%s:%s", host, port);
    }
  }

}
