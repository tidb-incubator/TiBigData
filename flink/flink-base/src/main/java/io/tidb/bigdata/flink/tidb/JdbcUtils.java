/*
 * Copyright 2020 TiDB Project Authors.
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

package io.tidb.bigdata.flink.tidb;

import static io.tidb.bigdata.flink.tidb.TiDBBaseDynamicTableFactory.DATABASE_NAME;
import static io.tidb.bigdata.flink.tidb.TiDBBaseDynamicTableFactory.DATABASE_URL;
import static io.tidb.bigdata.flink.tidb.TiDBBaseDynamicTableFactory.PASSWORD;
import static io.tidb.bigdata.flink.tidb.TiDBBaseDynamicTableFactory.TABLE_NAME;
import static io.tidb.bigdata.flink.tidb.TiDBBaseDynamicTableFactory.USERNAME;
import static io.tidb.bigdata.jdbc.TiDBDriver.MYSQL_DRIVER_NAME;
import static io.tidb.bigdata.jdbc.TiDBDriver.TIDB_PREFIX;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_DRIVER_NAME;
import static org.apache.flink.util.Preconditions.checkArgument;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import org.apache.flink.connector.jdbc.dialect.MySQLDialect;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;

public class JdbcUtils {

  public static String rewriteJdbcUrlPath(String url, String database) {
    URI uri;
    try {
      uri = new URI(url.substring("jdbc:".length()));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
    String scheme = uri.getScheme();
    String host = uri.getHost();
    int port = uri.getPort();
    String path = uri.getPath();
    return url.replace(String.format("jdbc:%s://%s:%d%s", scheme, host, port, path),
        String.format("jdbc:%s://%s:%d/%s", scheme, host, port, database));
  }

  public static JdbcOptions getJdbcOptions(Map<String, String> properties) {
    // replace database name in database url
    String dbUrl = properties.get(DATABASE_URL.key());
    String databaseName = properties.get(DATABASE_NAME.key());
    String tableName = properties.get(TABLE_NAME.key());
    checkArgument(dbUrl.matches("jdbc:(mysql|tidb)://[^/]+:\\d+/.*"),
        "the format of database url does not match jdbc:(mysql|tidb)://host:port/.*");
    dbUrl = rewriteJdbcUrlPath(dbUrl, databaseName);
    String driverName = dbUrl.startsWith(TIDB_PREFIX) ? TIDB_DRIVER_NAME : MYSQL_DRIVER_NAME;
    // jdbc options
    return JdbcOptions.builder()
        .setDBUrl(dbUrl)
        .setTableName(tableName)
        .setUsername(properties.get(USERNAME.key()))
        .setPassword(properties.get(PASSWORD.key()))
        .setDialect(new MySQLDialect())
        .setDriverName(driverName)
        .build();
  }


}
