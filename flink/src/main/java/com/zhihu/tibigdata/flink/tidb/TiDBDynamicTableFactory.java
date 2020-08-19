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

package com.zhihu.tibigdata.flink.tidb;

import static com.zhihu.tibigdata.tidb.ClientConfig.DATABASE_URL;
import static com.zhihu.tibigdata.tidb.ClientConfig.PASSWORD;
import static com.zhihu.tibigdata.tidb.ClientConfig.USERNAME;

import com.google.common.collect.ImmutableSet;
import com.zhihu.tibigdata.tidb.ClientConfig;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;

public class TiDBDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

  public static final String IDENTIFIER = "tidb";

  public static String DATABASE_NAME = "tidb.database.name";

  public static String TABLE_NAME = "tidb.table.name";

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
    return ImmutableSet.of(
        ConfigOptions.key(DATABASE_URL).stringType().noDefaultValue(),
        ConfigOptions.key(USERNAME).stringType().noDefaultValue(),
        ConfigOptions.key(DATABASE_NAME).stringType().noDefaultValue(),
        ConfigOptions.key(TABLE_NAME).stringType().noDefaultValue()
    );
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return ImmutableSet.of(
        ConfigOptions.key(ClientConfig.PASSWORD).stringType().noDefaultValue(),
        ConfigOptions.key(ClientConfig.MAX_POOL_SIZE).stringType().noDefaultValue(),
        ConfigOptions.key(ClientConfig.MIN_IDLE_SIZE).stringType().noDefaultValue(),
        SINK_BUFFER_FLUSH_INTERVAL,
        SINK_BUFFER_FLUSH_MAX_ROWS,
        SINK_MAX_RETRIES
    );
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    return new TiDBDynamicTableSource(context.getCatalogTable().getSchema(),
        context.getCatalogTable().toProperties());
  }

  /**
   * see ${@link org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory}
   */
  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    CatalogTable table = context.getCatalogTable();
    TableSchema schema = table.getSchema();
    Map<String, String> properties = table.toProperties();
    JdbcOptions jdbcOptions = JdbcOptions.builder()
        .setDBUrl(properties.get(DATABASE_URL))
        .setTableName(properties.get(TABLE_NAME))
        .setUsername(properties.get(USERNAME))
        .setPassword(properties.get(PASSWORD))
        .build();
    JdbcExecutionOptions jdbcExecutionOptions = JdbcExecutionOptions.builder().build();
    JdbcDmlOptions jdbcDmlOptions = JdbcDmlOptions.builder()
        .withTableName(jdbcOptions.getTableName())
        .withDialect(jdbcOptions.getDialect())
        .withFieldNames(schema.getFieldNames())
        .withKeyFields(schema.getPrimaryKey().map(pk -> pk.getColumns().toArray(new String[0]))
            .orElse(null))
        .build();
    return new JdbcDynamicTableSink(jdbcOptions, jdbcExecutionOptions, jdbcDmlOptions,
        schema);
  }
}
