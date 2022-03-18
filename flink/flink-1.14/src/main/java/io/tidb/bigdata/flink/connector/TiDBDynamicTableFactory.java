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

package io.tidb.bigdata.flink.connector;

import static io.tidb.bigdata.flink.connector.TiDBOptions.DATABASE_NAME;
import static io.tidb.bigdata.flink.connector.TiDBOptions.STREAMING_SOURCE;
import static io.tidb.bigdata.flink.connector.TiDBOptions.TABLE_NAME;
import static io.tidb.bigdata.flink.connector.TiDBOptions.WRITE_MODE;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.LOOKUP_CACHE_TTL;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.LOOKUP_MAX_RETRIES;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_MAX_RETRIES;

import com.google.common.collect.ImmutableSet;
import io.tidb.bigdata.flink.connector.TiDBOptions.SinkImpl;
import io.tidb.bigdata.flink.connector.sink.TiDBSinkOptions;
import io.tidb.bigdata.flink.connector.utils.JdbcUtils;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.TiDBWriteMode;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
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

  @Override
  public String factoryIdentifier() {
    throw new UnsupportedOperationException("TiDB factory is only work for catalog.");
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return TiDBOptions.requiredOptions();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return TiDBOptions.optionalOptions();
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
  public DynamicTableSink createDynamicTableSink(Context context) {
    // check fields
    FactoryUtil.TableFactoryHelper helper = FactoryUtil
        .createTableFactoryHelper(this, context);
    ReadableConfig config = helper.getOptions();
    TiDBSinkOptions tiDBSinkOptions = new TiDBSinkOptions(config);

    if (tiDBSinkOptions.getSinkImpl() == SinkImpl.TIKV) {
      return new TiDBDynamicTableSink(config.get(DATABASE_NAME), config.get(TABLE_NAME),
          context.getCatalogTable(), tiDBSinkOptions);

    } else if (tiDBSinkOptions.getSinkImpl() == SinkImpl.JDBC) {
      TableSchema schema = TableSchemaUtils.getPhysicalSchema(
          context.getCatalogTable().getSchema());
      String databaseName = config.get(DATABASE_NAME);
      // jdbc options
      JdbcConnectorOptions jdbcOptions = JdbcUtils.getJdbcOptions(
          context.getCatalogTable().toProperties());
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

    } else {
      throw new UnsupportedOperationException("Unsupported sink impl: " + tiDBSinkOptions.getSinkImpl());
    }
  }


  private String[] getKeyFields(Context context, ReadableConfig config, String databaseName,
      String tableName) {
    // check write mode
    TiDBWriteMode writeMode = TiDBWriteMode.fromString(config.get(WRITE_MODE));
    String[] keyFields = null;
    if (writeMode == TiDBWriteMode.UPSERT) {
      try (ClientSession clientSession = ClientSession.create(
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
}