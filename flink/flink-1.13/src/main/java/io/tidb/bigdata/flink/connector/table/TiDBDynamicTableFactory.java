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

import static io.tidb.bigdata.flink.connector.source.TiDBOptions.DATABASE_NAME;
import static io.tidb.bigdata.flink.connector.source.TiDBOptions.STREAMING_SOURCE;
import static io.tidb.bigdata.flink.connector.source.TiDBOptions.TABLE_NAME;
import static io.tidb.bigdata.flink.connector.source.TiDBOptions.WRITE_MODE;
import static io.tidb.bigdata.flink.connector.table.TiDBDynamicTableFactory.SinkImpl.JDBC;
import static io.tidb.bigdata.flink.connector.table.TiDBDynamicTableFactory.SinkTransaction.GLOBAL;
import static io.tidb.bigdata.flink.connector.table.TiDBDynamicTableFactory.SinkTransaction.LOCAL;

import com.google.common.collect.ImmutableSet;
import io.tidb.bigdata.flink.connector.sink.TiDBSinkOptions;
import io.tidb.bigdata.flink.connector.source.TiDBOptions;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.TiDBWriteMode;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

public class TiDBDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

  public static final String IDENTIFIER = "tidb";

  /**
   * see ${@link org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory}
   */
  public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS = ConfigOptions
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
  public static final ConfigOption<SinkImpl> SINK_IMPL =
      ConfigOptions.key("tidb.sink.impl")
          .enumType(SinkImpl.class)
          .defaultValue(JDBC);
  public static final ConfigOption<SinkTransaction> SINK_TRANSACTION =
      ConfigOptions.key("tidb.sink.transaction")
          .enumType(SinkTransaction.class)
          .defaultValue(LOCAL);
  public static final ConfigOption<Integer> SINK_MAX_RETRIES = ConfigOptions
      .key("sink.max-retries")
      .intType()
      .defaultValue(3)
      .withDescription("the max retry times if writing records to database failed.");
  public static final ConfigOption<Integer> SINK_BUFFER_SIZE = ConfigOptions
      .key("tidb.sink.buffer-size")
      .intType()
      .defaultValue(1000);
  public static final ConfigOption<Integer> ROW_ID_ALLOCATOR_STEP = ConfigOptions
      .key("tidb.sink.row-id-allocator.step")
      .intType()
      .defaultValue(30000);
  public static final ConfigOption<Boolean> UNBOUNDED_SOURCE_USE_CHECKPOINT_SINK = ConfigOptions
      .key("tidb.sink.unbounded-source-use-checkpoint-sink")
      .booleanType()
      .defaultValue(true);
  public static final ConfigOption<Boolean> IGNORE_AUTOINCREMENT_COLUMN_VALUE = ConfigOptions
      .key("tidb.sink.ignore-autoincrement-column-value")
      .booleanType()
      .defaultValue(false)
      .withDescription("If true, "
          + "for autoincrement column, we will generate value instead of the the actual value. "
          + "And if false, the value of autoincrement column can not be null");
  // look up config options
  public static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS = ConfigOptions
      .key("lookup.cache.max-rows")
      .longType()
      .defaultValue(-1L)
      .withDescription("the max number of rows of lookup cache, over this value, "
          + "the oldest rows will be eliminated. \"cache.max-rows\" and \"cache.ttl\" options "
          + "must all be specified if any of them is specified. "
          + "Cache is not enabled as default.");
  public static final ConfigOption<Duration> LOOKUP_CACHE_TTL = ConfigOptions
      .key("lookup.cache.ttl")
      .durationType()
      .defaultValue(Duration.ofSeconds(10))
      .withDescription("the cache time to live.");
  public static final ConfigOption<Integer> LOOKUP_MAX_RETRIES = ConfigOptions
      .key("lookup.max-retries")
      .intType()
      .defaultValue(3)
      .withDescription("the max retry times if lookup database failed.");

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return TiDBOptions.requiredOptions();
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    ReadableConfig config = helper.getOptions();
    return new TiDBDynamicTableSource(context.getCatalogTable(),
        config.getOptional(STREAMING_SOURCE).isPresent()
            ? ChangelogMode.all() : ChangelogMode.insertOnly(),
        new JdbcLookupOptions(
            config.get(LOOKUP_CACHE_MAX_ROWS),
            config.get(LOOKUP_CACHE_TTL).toMillis(),
            config.get(LOOKUP_MAX_RETRIES)));
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return TiDBOptions.withMoreOptionalOptions(
        SINK_BUFFER_FLUSH_INTERVAL,
        SINK_BUFFER_FLUSH_MAX_ROWS,
        SINK_MAX_RETRIES);
  }

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    // check fields
    FactoryUtil.TableFactoryHelper helper = FactoryUtil
        .createTableFactoryHelper(this, context);
    ReadableConfig config = helper.getOptions();
    TiDBSinkOptions tiDBSinkOptions = new TiDBSinkOptions(config);
    SinkTransaction sinkTransaction = config.get(SINK_TRANSACTION);
    if (tiDBSinkOptions.getSinkImpl() == SinkImpl.TIKV) {
      return new TiDBDynamicTableSink(config.get(DATABASE_NAME), config.get(TABLE_NAME),
          context.getCatalogTable(), tiDBSinkOptions);
    }
    if (sinkTransaction == GLOBAL) {
      throw new IllegalArgumentException("Global transaction is not supported in JDBC sink!");
    }
    TableSchema schema = TableSchemaUtils.getPhysicalSchema(
        context.getCatalogTable().getSchema());
    String databaseName = config.get(DATABASE_NAME);
    // jdbc options
    JdbcOptions jdbcOptions = JdbcUtils.getJdbcOptions(context.getCatalogTable().toProperties());
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
        .withKeyFields(getKeyFields(context, config, databaseName, jdbcOptions.getTableName()))
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

  public enum SinkImpl {
    JDBC, TIKV;

    public static SinkImpl fromString(String s) {
      for (SinkImpl value : values()) {
        if (value.name().equalsIgnoreCase(s)) {
          return value;
        }
      }
      throw new IllegalArgumentException(
          "Property sink.impl must be one of: " + Arrays.toString(values()));
    }
  }

  public enum SinkTransaction {
    LOCAL, GLOBAL;

    public static SinkTransaction fromString(String s) {
      for (SinkTransaction value : values()) {
        if (value.name().equalsIgnoreCase(s)) {
          return value;
        }
      }
      throw new IllegalArgumentException(
          "Property sink.transaction must be one of: " + Arrays.toString(values()));
    }
  }
}