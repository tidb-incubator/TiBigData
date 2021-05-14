/*
 * Copyright 2021 TiKV Project Authors.
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

package io.tidb.bigdata.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class TiDBDriverTest {

  public static final String JDBC_DRIVER = "io.tidb.bigdata.jdbc.TiDBDriver";

  public static final String TIDB_HOST = "TIDB_HOST";

  public static final String TIDB_PORT = "TIDB_PORT";

  public static final String TIDB_USER = "TIDB_USER";

  public static final String TIDB_PASSWORD = "TIDB_PASSWORD";

  public static final String tidbHost = getEnvOrDefault(TIDB_HOST, "127.0.0.1");

  public static final String tidbPort = getEnvOrDefault(TIDB_PORT, "4000");

  public static final String tidbUser = getEnvOrDefault(TIDB_USER, "root");

  public static final String tidbPassword = getEnvOrDefault(TIDB_PASSWORD, "");

  private static String getEnvOrDefault(String key, String default0) {
    String tmp = System.getenv(key);
    if (tmp != null && !tmp.equals("")) {
      return tmp;
    }

    tmp = System.getProperty(key);
    if (tmp != null && !tmp.equals("")) {
      return tmp;
    }

    return default0;
  }

  public static final String tidbUrl = String
      .format("jdbc:tidb://%s:%s?user=%s&password=%s", tidbHost, tidbPort, tidbUser,
          tidbPassword);

  private Connection conn = null;

  @Test
  public void testTiDBDriver() throws ClassNotFoundException, SQLException {
    Class.forName(JDBC_DRIVER);
    conn = DriverManager.getConnection(tidbUrl);
    executeUpdate("create database if not exists test");
    executeUpdate("drop table if exists test.t");
    executeUpdate("create table test.t(id int)");
    executeUpdate("insert into test.t values(0),(1),(2)");

    List<List<Object>> result = executeQuery("select count(*) from test.t");
    Assert.assertEquals(result.get(0).get(0), 3L);

    conn.close();
  }

  private void executeUpdate(String sql) throws SQLException {
    try (Statement tidbStmt = conn.createStatement()) {
      tidbStmt.executeUpdate(sql);
    }
  }

  private List<List<Object>> executeQuery(String sql) throws SQLException {
    ArrayList<List<Object>> result = new ArrayList<>();

    try (Statement tidbStmt = conn.createStatement()) {
      ResultSet resultSet = tidbStmt.executeQuery(sql);
      ResultSetMetaData rsMetaData = resultSet.getMetaData();

      while (resultSet.next()) {
        ArrayList<Object> row = new ArrayList<>();
        for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
          row.add(resultSet.getObject(i));
        }
        result.add(row);
      }
    }
    return result;
  }
}
