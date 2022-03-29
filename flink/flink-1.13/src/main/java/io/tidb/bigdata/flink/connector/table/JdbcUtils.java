/*
 * Copyright 2021 TiDB Project Authors.
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

package io.tidb.bigdata.flink.connector.table;

import static io.tidb.bigdata.flink.connector.source.TiDBOptions.ASYNC_MODE;
import static io.tidb.bigdata.flink.connector.source.TiDBOptions.DATABASE_NAME;
import static io.tidb.bigdata.flink.connector.source.TiDBOptions.DATABASE_URL;
import static io.tidb.bigdata.flink.connector.source.TiDBOptions.LOOKUP_MAX_POOL;
import static io.tidb.bigdata.flink.connector.source.TiDBOptions.PASSWORD;
import static io.tidb.bigdata.flink.connector.source.TiDBOptions.TABLE_NAME;
import static io.tidb.bigdata.flink.connector.source.TiDBOptions.USERNAME;
import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkArgument;

import io.tidb.bigdata.flink.connector.table.AsyncLookupOptions.Builder;
import io.tidb.bigdata.jdbc.TiDBDriver;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
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
    String driverName = TiDBDriver.driverForUrl(dbUrl);
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

  public static AsyncLookupOptions getAsyncJdbcOptions(Map<String, String> properties) {
    Builder builder = AsyncLookupOptions.builder();
    String mode = properties.get(ASYNC_MODE.key());
    String poolSize = properties.get(LOOKUP_MAX_POOL.key());
    if (poolSize != null) {
      int maxPoolSize = Integer.parseInt(poolSize);
      builder.setMaxPoolSize(maxPoolSize);
    }
    if (mode != null) {
      boolean async = "true".equals(mode);
      builder.setAsync(async);
    }
    // jdbc options
    return builder.build();
  }

  public static String quoteIdentifier(String identifier) {
    return "`" + identifier + "`";
  }

  public static String getSelectFromStatement(
      String tableName, String[] selectFields, String[] conditionFields) {
    String selectExpressions =
        Arrays.stream(selectFields)
            .map(JdbcUtils::quoteIdentifier)
            .collect(Collectors.joining(", "));
    String fieldExpressions =
        Arrays.stream(conditionFields)
            .map(f -> format("%s = ?", quoteIdentifier(f)))
            .collect(Collectors.joining(" AND "));
    return "SELECT "
        + selectExpressions
        + " FROM "
        + quoteIdentifier(tableName)
        + (conditionFields.length > 0 ? " WHERE " + fieldExpressions : "");
  }


}