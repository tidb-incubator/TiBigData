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
import static org.apache.flink.util.Preconditions.checkArgument;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.tidb.bigdata.flink.connector.TiDBOptions.SinkImpl;
import io.tidb.bigdata.flink.connector.sink.TiDBSinkOptions;
import io.tidb.bigdata.flink.connector.utils.JdbcUtils;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.TiDBWriteMode;
import io.tidb.bigdata.tidb.meta.TiColumnInfo;
import io.tidb.bigdata.tidb.meta.TiTableInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSink;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableColumn.MetadataColumn;
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
    throw new UnsupportedOperationException(
        "TiDB table factory is not supported anymore, please use catalog.");
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
    return new TiDBDynamicTableSource(
        context.getCatalogTable(),
        config.getOptional(STREAMING_SOURCE).isPresent()
            ? ChangelogMode.all()
            : ChangelogMode.insertOnly(),
        new JdbcLookupOptions(
            config.get(LOOKUP_CACHE_MAX_ROWS),
            config.get(LOOKUP_CACHE_TTL).toMillis(),
            config.get(LOOKUP_MAX_RETRIES)));
  }

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    // Metadata columns is not real columns, should not be created for sink.
    if (context.getCatalogTable().getSchema().getTableColumns().stream()
        .anyMatch(column -> column instanceof MetadataColumn)) {
      throw new IllegalStateException("Metadata columns is not supported for sink");
    }
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    ReadableConfig config = helper.getOptions();
    TiDBSinkOptions tiDBSinkOptions = new TiDBSinkOptions(config);

    if (tiDBSinkOptions.getSinkImpl() == SinkImpl.TIKV) {
      return new TiDBDynamicTableSink(
          config.get(DATABASE_NAME),
          config.get(TABLE_NAME),
          context.getCatalogTable(),
          tiDBSinkOptions);
    } else if (tiDBSinkOptions.getSinkImpl() == SinkImpl.JDBC) {
      TableSchema schema =
          TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
      String databaseName = config.get(DATABASE_NAME);
      // jdbc options
      JdbcConnectorOptions jdbcOptions =
          JdbcUtils.getJdbcOptions(context.getCatalogTable().toProperties());
      // execution options
      JdbcExecutionOptions jdbcExecutionOptions =
          JdbcExecutionOptions.builder()
              .withBatchSize(config.get(SINK_BUFFER_FLUSH_MAX_ROWS))
              .withBatchIntervalMs(config.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis())
              .withMaxRetries(config.get(SINK_MAX_RETRIES))
              .build();
      ColumnKeyField columnKeyField =
          getKeyFields(context, config, databaseName, jdbcOptions.getTableName());

      if (tiDBSinkOptions.getUpdateColumns() != null) {
        // dml options
        JdbcDmlOptions jdbcDmlOptions =
            JdbcDmlOptions.builder()
                .withTableName(jdbcOptions.getTableName())
                .withDialect(jdbcOptions.getDialect())
                .withFieldNames(schema.getFieldNames())
                .build();

        String[] updateColumnNames = tiDBSinkOptions.getUpdateColumns().split("\\s*,\\s*");

        validationForInsertOnDuplicateUpdate(tiDBSinkOptions, columnKeyField, updateColumnNames);

        List<TableColumn> updateColumns = new ArrayList<>();
        int[] updateColumnIndexes =
            getUpdateColumnAndIndexes(
                schema, databaseName, jdbcOptions, updateColumnNames, updateColumns);
        return new InsertOnDuplicateUpdateSink(
            jdbcOptions,
            jdbcExecutionOptions,
            jdbcDmlOptions,
            schema,
            updateColumns,
            updateColumnIndexes);
      } else {
        // dml option
        JdbcDmlOptions jdbcDmlOptions =
            JdbcDmlOptions.builder()
                .withTableName(jdbcOptions.getTableName())
                .withDialect(jdbcOptions.getDialect())
                .withFieldNames(schema.getFieldNames())
                .withKeyFields(columnKeyField.getKeyFieldFlatMap())
                .build();

        return new JdbcDynamicTableSink(jdbcOptions, jdbcExecutionOptions, jdbcDmlOptions, schema);
      }
    } else {
      throw new UnsupportedOperationException(
          "Unsupported sink impl: " + tiDBSinkOptions.getSinkImpl());
    }
  }

  private void validationForInsertOnDuplicateUpdate(
      TiDBSinkOptions tiDBSinkOptions, ColumnKeyField columnKeyField, String[] updateColumnNames) {
    checkArgument(
        tiDBSinkOptions.getWriteMode() == TiDBWriteMode.UPSERT,
        "Insert on duplicate only work in `upsert` mode.");

    /**
     * `UPDATE t1 SET c=c+1 WHERE a=1 OR b=2 LIMIT 1;` If a=1 OR b=2 matches several rows, only one
     * row is updated. In general, you should try to avoid using an ON DUPLICATE KEY UPDATE clause
     * on tables with multiple unique indexes.
     *
     * @see <a
     *     href=https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html>insert-on-duplicate</a>
     * @see <a href=https://github.com/pingcap/tidb/issues/34813>issue-ON-DUPLICATE-KEY</a>
     *     <p>The constraints are as follows: - the destination table should contain only one
     *     not-null unique key(including primary key). - Multiple-Column Indexes should be all
     *     not-null. - the update columns should contain the unique key column(including primary
     *     key).
     */
    if (!tiDBSinkOptions.isSkipCheckForUpdateColumns()) {
      List<List<String>> keyFields =
          Lists.newArrayList(columnKeyField.getUniqueKeys().orElse(Collections.emptyList()));
      List<String> primaryKey = columnKeyField.getPrimaryKey().orElse(Collections.emptyList());
      if (!primaryKey.isEmpty()) {
        keyFields.add(primaryKey);
      }

      checkArgument(
          keyFields.size() == 1,
          "Sink table should only have one unique key or primary key\n"
              + "If you want to force skip the constraint, "
              + "set `tidb.sink.skip-check-update-columns` to true");

      TiTableInfo table =
          columnKeyField
              .getTableInfo()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          String.format(
                              "Failed to get tableInfo for table %s.%s",
                              columnKeyField.getDatabaseName(), columnKeyField.getTableName())));
      for (String keyField : Objects.requireNonNull(columnKeyField.getKeyFieldFlatMap())) {
        TiColumnInfo column = table.getColumn(keyField);
        checkArgument(
            column.getType().isNotNull(),
            "Unique key or primary key should be not null\n"
                + "If you want to force skip the constraint, "
                + "set `tidb.sink.skip-check-update-columns` to true");
      }

      ArrayList<String> updateColumns = Lists.newArrayList(updateColumnNames);
      List<List<String>> collect =
          keyFields.stream().filter(updateColumns::containsAll).collect(Collectors.toList());
      checkArgument(
          collect.size() == 1,
          "Update columns should contains all unique key columns or primary key columns\n"
              + "If you want to force skip the constraint, "
              + "set `tidb.sink.skip-check-update-columns` to true");
    }
  }

  private int[] getUpdateColumnAndIndexes(
      TableSchema schema,
      String databaseName,
      JdbcConnectorOptions jdbcOptions,
      String[] updateColumnNames,
      List<TableColumn> updateColumns) {
    int[] index = new int[updateColumnNames.length];
    for (int i = 0; i < updateColumnNames.length; i++) {
      String updateColumnName = updateColumnNames[i];
      Optional<TableColumn> tableColumn = schema.getTableColumn(updateColumnName);
      if (!tableColumn.isPresent()) {
        throw new IllegalStateException(
            String.format(
                "Unknown updateColumn %s in table %s.%s",
                updateColumnName, databaseName, jdbcOptions.getTableName()));
      } else {
        updateColumns.add(tableColumn.get());
        index[i] = schema.getTableColumns().indexOf(tableColumn.get());
      }
    }
    return index;
  }

  private ColumnKeyField getKeyFields(
      Context context, ReadableConfig config, String databaseName, String tableName) {
    // check write mode
    TiDBWriteMode writeMode = TiDBWriteMode.fromString(config.get(WRITE_MODE));
    ColumnKeyField columnKeyField = new ColumnKeyField(databaseName, tableName);
    if (writeMode == TiDBWriteMode.UPSERT) {
      try (ClientSession clientSession =
          ClientSession.create(new ClientConfig(context.getCatalogTable().toProperties()))) {

        columnKeyField.setUniqueKeys(clientSession.getUniqueKeyColumns(databaseName, tableName));
        columnKeyField.setPrimaryKey(clientSession.getPrimaryKeyColumns(databaseName, tableName));
        columnKeyField.setTableInfo(
            clientSession
                .getTable(databaseName, tableName)
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            String.format("Table %s.%s not exist", databaseName, tableName))));
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
    return columnKeyField;
  }

  private static class ColumnKeyField {

    private final String databaseName;
    private final String tableName;

    public ColumnKeyField(String databaseName, String tableName) {
      this.databaseName = databaseName;
      this.tableName = tableName;
    }

    private List<String> primaryKey;
    private List<List<String>> uniqueKeys;
    private TiTableInfo tableInfo;

    public String getDatabaseName() {
      return databaseName;
    }

    public String getTableName() {
      return tableName;
    }

    public void setPrimaryKey(List<String> primaryKey) {
      this.primaryKey = primaryKey;
    }

    public void setUniqueKeys(List<List<String>> uniqueKeys) {
      this.uniqueKeys = uniqueKeys;
    }

    public Optional<List<String>> getPrimaryKey() {
      return Optional.ofNullable(primaryKey);
    }

    public Optional<List<List<String>>> getUniqueKeys() {
      return Optional.ofNullable(uniqueKeys);
    }

    public Optional<TiTableInfo> getTableInfo() {
      return Optional.ofNullable(tableInfo);
    }

    public void setTableInfo(TiTableInfo tableInfo) {
      this.tableInfo = tableInfo;
    }

    public String[] getKeyFieldFlatMap() {
      Set<String> set =
          ImmutableSet.<String>builder()
              .addAll(
                  this.getUniqueKeys().orElse(Collections.emptyList()).stream()
                      .flatMap(List::stream)
                      .collect(Collectors.toList()))
              .addAll(this.getPrimaryKey().orElse(Collections.emptyList()))
              .build();
      return set.size() == 0 ? null : set.toArray(new String[0]);
    }
  }
}
