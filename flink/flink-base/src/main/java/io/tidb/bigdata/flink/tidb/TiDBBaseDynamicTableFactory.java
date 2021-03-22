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

import static io.tidb.bigdata.jdbc.TiDBDriver.MYSQL_DRIVER_NAME;
import static io.tidb.bigdata.jdbc.TiDBDriver.TIDB_PREFIX;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_DRIVER_NAME;
import static org.apache.flink.util.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.TiDBWriteMode;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.dialect.MySQLDialect;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

public abstract class TiDBBaseDynamicTableFactory implements DynamicTableSourceFactory,
    DynamicTableSinkFactory {

  public static final String IDENTIFIER = "tidb";

  private static final ConfigOption<String> DATABASE_URL = ConfigOptions
      .key(ClientConfig.DATABASE_URL).stringType().noDefaultValue();

  private static final ConfigOption<String> USERNAME = ConfigOptions.key(ClientConfig.USERNAME)
      .stringType().noDefaultValue();

  private static final ConfigOption<String> PASSWORD = ConfigOptions.key(ClientConfig.PASSWORD)
      .stringType().noDefaultValue();

  public static final ConfigOption<String> DATABASE_NAME = ConfigOptions
      .key("tidb.database.name").stringType().noDefaultValue();

  public static final ConfigOption<String> TABLE_NAME = ConfigOptions
      .key("tidb.table.name").stringType().noDefaultValue();

  private static final ConfigOption<String> MAX_POOL_SIZE = ConfigOptions
      .key(ClientConfig.MAX_POOL_SIZE).stringType().noDefaultValue();

  private static final ConfigOption<String> MIN_IDLE_SIZE = ConfigOptions
      .key(ClientConfig.MIN_IDLE_SIZE).stringType().noDefaultValue();

  private static final ConfigOption<String> WRITE_MODE = ConfigOptions
      .key(ClientConfig.TIDB_WRITE_MODE).stringType()
      .defaultValue(ClientConfig.TIDB_WRITE_MODE_DEFAULT);

  /**
   * see ${@link org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory}
   */
  private static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS = ConfigOptions
      .key("sink.buffer-flush.max-rows")
      .intType()
      .defaultValue(100)
      .withDescription(
          "the flush max size (includes all append, upsert and delete records), over this number"
              + " of records, will flush data. The default value is 100.");
  private static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL = ConfigOptions
      .key("sink.buffer-flush.interval")
      .durationType()
      .defaultValue(Duration.ofSeconds(1))
      .withDescription(
          "the flush interval mills, over this time, asynchronous threads will flush data. The "
              + "default value is 1s.");
  private static final ConfigOption<Integer> SINK_MAX_RETRIES = ConfigOptions
      .key("sink.max-retries")
      .intType()
      .defaultValue(3)
      .withDescription("the max retry times if writing records to database failed.");

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return ImmutableSet.of(DATABASE_URL, USERNAME, DATABASE_NAME, TABLE_NAME);
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return ImmutableSet.of(
        PASSWORD,
        MAX_POOL_SIZE,
        MIN_IDLE_SIZE,
        SINK_BUFFER_FLUSH_INTERVAL,
        SINK_BUFFER_FLUSH_MAX_ROWS,
        SINK_MAX_RETRIES,
        WRITE_MODE
    );
  }

  /**
   * see ${@link org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory}
   */
  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    FactoryUtil.TableFactoryHelper helper = FactoryUtil
        .createTableFactoryHelper(this, context);
    ReadableConfig config = helper.getOptions();
    TableSchema schema = context.getCatalogTable().getSchema();

    // replace database name in database url
    String dbUrl = config.get(DATABASE_URL);
    String databaseName = config.get(DATABASE_NAME);
    String tableName = config.get(TABLE_NAME);
    checkArgument(dbUrl.matches("jdbc:(mysql|tidb)://[^/]+:\\d+/.*"),
        "the format of database url does not match jdbc:(mysql|tidb)://host:port/.*");
    dbUrl = rewriteJdbcUrlPath(dbUrl, databaseName);
    String driverName = dbUrl.startsWith(TIDB_PREFIX) ? TIDB_DRIVER_NAME : MYSQL_DRIVER_NAME;
    // jdbc options
    JdbcOptions jdbcOptions = JdbcOptions.builder()
        .setDBUrl(dbUrl)
        .setTableName(tableName)
        .setUsername(config.get(USERNAME))
        .setPassword(config.get(PASSWORD))
        .setDialect(new MySQLDialect())
        .setDriverName(driverName)
        .build();
    // execution options
    JdbcExecutionOptions jdbcExecutionOptions = JdbcExecutionOptions.builder()
        .withBatchSize(config.get(SINK_BUFFER_FLUSH_MAX_ROWS))
        .withBatchIntervalMs(config.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis())
        .withMaxRetries(config.get(SINK_MAX_RETRIES))
        .build();
    // dml options
    JdbcDmlOptions jdbcDmlOptions = JdbcDmlOptions.builder()
        .withTableName(jdbcOptions.getTableName())
        .withDialect(jdbcOptions.getDialect())
        .withFieldNames(schema.getFieldNames())
        .withKeyFields(getKeyFields(context, config, databaseName, tableName))
        .build();

    return new JdbcDynamicTableSink(jdbcOptions, jdbcExecutionOptions, jdbcDmlOptions, schema);
  }

  private String[] getKeyFields(Context context, ReadableConfig config, String databaseName,
      String tableName) {
    // check write mode
    TiDBWriteMode writeMode = TiDBWriteMode.fromString(config.get(WRITE_MODE));
    String[] keyFields = null;
    if (writeMode == TiDBWriteMode.UPSERT) {
      try (ClientSession clientSession = ClientSession.createWithSingleConnection(
          new ClientConfig(context.getCatalogTable().toProperties()))) {
        Set<String> set = ImmutableSet.<String>builder()
            .addAll(clientSession.getUniqueKeyColumns(databaseName, tableName))
            .addAll(clientSession.getPrimaryKeyColumns(databaseName, tableName))
            .build();
        keyFields = set.size() == 0 ? null : set.toArray(new String[0]);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
    return keyFields;
  }

  @VisibleForTesting
  protected String rewriteJdbcUrlPath(String url, String database) {
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
}
