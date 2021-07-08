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

import static io.tidb.bigdata.flink.tidb.TiDBConfigOptions.DATABASE_NAME;
import static io.tidb.bigdata.flink.tidb.TiDBConfigOptions.DATABASE_URL;
import static io.tidb.bigdata.flink.tidb.TiDBConfigOptions.IDENTIFIER;
import static io.tidb.bigdata.flink.tidb.TiDBConfigOptions.LOOKUP_CACHE_MAX_ROWS;
import static io.tidb.bigdata.flink.tidb.TiDBConfigOptions.LOOKUP_CACHE_TTL;
import static io.tidb.bigdata.flink.tidb.TiDBConfigOptions.LOOKUP_MAX_RETRIES;
import static io.tidb.bigdata.flink.tidb.TiDBConfigOptions.MAX_POOL_SIZE;
import static io.tidb.bigdata.flink.tidb.TiDBConfigOptions.MIN_IDLE_SIZE;
import static io.tidb.bigdata.flink.tidb.TiDBConfigOptions.PASSWORD;
import static io.tidb.bigdata.flink.tidb.TiDBConfigOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static io.tidb.bigdata.flink.tidb.TiDBConfigOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static io.tidb.bigdata.flink.tidb.TiDBConfigOptions.SINK_MAX_RETRIES;
import static io.tidb.bigdata.flink.tidb.TiDBConfigOptions.TABLE_NAME;
import static io.tidb.bigdata.flink.tidb.TiDBConfigOptions.USERNAME;
import static io.tidb.bigdata.flink.tidb.TiDBConfigOptions.WRITE_MODE;

import com.google.common.collect.ImmutableSet;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.TiDBWriteMode;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

public abstract class TiDBBaseDynamicTableFactory implements DynamicTableSourceFactory,
    DynamicTableSinkFactory {

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return ImmutableSet.of(DATABASE_URL, USERNAME, DATABASE_NAME);
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return withMoreOptionalOptions();
  }

  protected Set<ConfigOption<?>> withMoreOptionalOptions(ConfigOption... options) {
    return ImmutableSet.<ConfigOption<?>>builder().add(
        TABLE_NAME,
        PASSWORD,
        MAX_POOL_SIZE,
        MIN_IDLE_SIZE,
        SINK_BUFFER_FLUSH_INTERVAL,
        SINK_BUFFER_FLUSH_MAX_ROWS,
        SINK_MAX_RETRIES,
        WRITE_MODE)
        .add(options)
        .build();
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

  protected JdbcLookupOptions getLookupOptions(Context context) {
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    ReadableConfig config = helper.getOptions();
    return new JdbcLookupOptions(
        config.get(LOOKUP_CACHE_MAX_ROWS),
        config.get(LOOKUP_CACHE_TTL).toMillis(),
        config.get(LOOKUP_MAX_RETRIES));
  }
}
