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

package io.tidb.bigdata.flink.connector.catalog;

import static io.tidb.bigdata.flink.tidb.TiDBBaseDynamicTableFactory.DATABASE_NAME;
import static io.tidb.bigdata.flink.tidb.TiDBBaseDynamicTableFactory.TABLE_NAME;

import com.google.common.collect.ImmutableList;
import io.tidb.bigdata.flink.connector.source.TiDBMetadata;
import io.tidb.bigdata.flink.connector.source.TiDBSchemaAdapter;
import io.tidb.bigdata.flink.connector.table.TiDBDynamicTableFactory;
import io.tidb.bigdata.flink.telemetry.AsyncTelemetry;
import io.tidb.bigdata.flink.tidb.TiDBBaseCatalog;
import io.tidb.bigdata.flink.tidb.TypeUtils;
import io.tidb.bigdata.tidb.meta.TiColumnInfo;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.types.DataType;

public class TiDBCatalog extends TiDBBaseCatalog {

  public static final String TIDB_TELEMETRY_ENABLE = "tidb.telemetry.enable";
  public static final String TIDB_TELEMETRY_ENABLE_DEFAULT = "true";

  public TiDBCatalog(String name, String defaultDatabase, Map<String, String> properties) {
    super(name, defaultDatabase, properties);
  }

  public TiDBCatalog(String name, Map<String, String> properties) {
    super(name, properties);

    // Report telemetry.
    if (Boolean.parseBoolean(
        properties.getOrDefault(TIDB_TELEMETRY_ENABLE, TIDB_TELEMETRY_ENABLE_DEFAULT))) {
      AsyncTelemetry asyncTelemetry = new AsyncTelemetry(properties);
      asyncTelemetry.report();
    }
  }

  public TiDBCatalog(Map<String, String> properties) {
    super(properties);
  }

  @Override
  public Optional<Factory> getFactory() {
    return Optional.of(new TiDBDynamicTableFactory());
  }

  @Override
  public CatalogBaseTable getTable(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    String databaseName = tablePath.getDatabaseName();
    String tableName = tablePath.getObjectName();
    Map<String, String> properties = new HashMap<>(this.properties);
    properties.put(DATABASE_NAME.key(), databaseName);
    properties.put(TABLE_NAME.key(), tableName);
    return CatalogTable.of(getSchema(databaseName, tableName), "", ImmutableList.of(), properties);
  }

  public Schema getSchema(String databaseName, String tableName) {
    Schema.Builder builder = Schema.newBuilder();
    List<TiColumnInfo> columns =
        getClientSession().getTableMust(databaseName, tableName).getColumns();
    columns.forEach(
        column -> {
          DataType flinkType = TypeUtils.getFlinkType(column.getType());
          flinkType = column.getType().isNotNull() ? flinkType.notNull() : flinkType.nullable();
          builder.column(column.getName(), flinkType);
        });
    LinkedHashMap<String, TiDBMetadata> metadata =
        TiDBSchemaAdapter.parseMetadataColumns(properties);
    metadata.forEach(
        (name, meta) -> builder.columnByMetadata(name, meta.getType(), meta.getKey(), false));
    List<String> primaryKeyColumns =
        getClientSession().getPrimaryKeyColumns(databaseName, tableName);
    if (primaryKeyColumns.size() > 0) {
      builder.primaryKey(primaryKeyColumns);
    }
    return builder.build();
  }

  public int queryTableCount(String databaseName, String tableName) {
    return getClientSession().queryTableCount(databaseName, tableName);
  }
}
