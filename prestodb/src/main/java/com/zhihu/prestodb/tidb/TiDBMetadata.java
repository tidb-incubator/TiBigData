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

package com.zhihu.prestodb.tidb;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.zhihu.prestodb.tidb.TypeHelpers.getHelper;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.zhihu.presto.tidb.ColumnHandleInternal;
import com.zhihu.presto.tidb.MetadataInternal;
import com.zhihu.presto.tidb.Wrapper;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import javax.inject.Inject;

public final class TiDBMetadata extends Wrapper<MetadataInternal> implements ConnectorMetadata {

  @Inject
  public TiDBMetadata(TiDBConnectorId connectorId, TiDBSession session) {
    super(new MetadataInternal(connectorId.toString(), session.getInternal()));
  }

  private static boolean isColumnTypeSupported(ColumnHandleInternal handle) {
    return getHelper(handle.getType()).isPresent();
  }

  private static ColumnMetadata createColumnMetadata(TiDBColumnHandle handle) {
    return new ColumnMetadata(handle.getName(), handle.getPrestoType());
  }

  @Override
  public List<String> listSchemaNames(ConnectorSession session) {
    return getInternal().listSchemaNames();
  }

  @Override
  public TiDBTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
    return getInternal().getTableHandle(tableName.getSchemaName(), tableName.getTableName())
        .map(TiDBTableHandle::new).orElse(null);
  }

  @Override
  public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session,
      ConnectorTableHandle table, Constraint<ColumnHandle> constraint,
      Optional<Set<ColumnHandle>> desiredColumns) {
    TiDBTableHandle tableHandle = (TiDBTableHandle) table;
    ConnectorTableLayout layout = new ConnectorTableLayout(
        new TiDBTableLayoutHandle(tableHandle, Optional.of(constraint.getSummary()),
            Optional.empty()));
    return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
  }

  @Override
  public ConnectorTableLayout getTableLayout(ConnectorSession session,
      ConnectorTableLayoutHandle handle) {
    return new ConnectorTableLayout(handle);
  }

  @Override
  public ConnectorTableMetadata getTableMetadata(ConnectorSession session,
      ConnectorTableHandle table) {
    TiDBTableHandle tableHandle = (TiDBTableHandle) table;
    checkArgument(tableHandle.getConnectorId().equals(getInternal().getConnectorId()),
        "tableHandle is not for this connector");
    return getTableMetadata(
        new SchemaTableName(tableHandle.getSchemaName(), tableHandle.getTableName()));
  }

  private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTable) {
    return new ConnectorTableMetadata(schemaTable,
        getTableMetadataStream(schemaTable).collect(toImmutableList()));
  }

  @Override
  public List<SchemaTableName> listTables(ConnectorSession session, String schemaName) {
    return getInternal().listTables(Optional.ofNullable(schemaName)).entrySet().stream()
        .flatMap(entry -> {
          String schema = entry.getKey();
          return entry.getValue().stream().map(table -> new SchemaTableName(schema, table));
        }).collect(toImmutableList());
  }

  private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix) {
    List<SchemaTableName> tables = listTables(session, prefix.getSchemaName());
    String tablePrefix = prefix.getTableName();
    if (tablePrefix != null) {
      return tables.stream().filter(t -> t.getTableName().startsWith(tablePrefix))
          .collect(toImmutableList());
    }
    return tables;
  }

  private Stream<TiDBColumnHandle> getColumnHandlesStream(String schemaName, String tableName) {
    return getInternal().getColumnHandles(schemaName, tableName).map(
        handles -> handles.stream().filter(TiDBMetadata::isColumnTypeSupported)
            .map(TiDBColumnHandle::new)).orElseGet(Stream::empty);
  }

  @Override
  public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session,
      ConnectorTableHandle tableHandle) {
    TiDBTableHandle handle = (TiDBTableHandle) tableHandle;
    return getColumnHandlesStream(handle.getSchemaName(), handle.getTableName())
        .collect(toImmutableMap(TiDBColumnHandle::getName, identity()));
  }

  @Override
  public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
      SchemaTablePrefix prefix) {
    requireNonNull(prefix, "prefix is null");
    ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
    for (SchemaTableName tableName : listTables(session, prefix)) {
      columns.put(tableName, getTableMetadataStream(tableName).collect(toImmutableList()));
    }
    return columns.build();
  }

  private Stream<ColumnMetadata> getTableMetadataStream(SchemaTableName schemaTable) {
    return getTableMetadataStream(schemaTable.getSchemaName(), schemaTable.getTableName());
  }

  private Stream<ColumnMetadata> getTableMetadataStream(String schemaName, String tableName) {
    return getColumnHandlesStream(schemaName, tableName).map(TiDBMetadata::createColumnMetadata);
  }

  @Override
  public ColumnMetadata getColumnMetadata(ConnectorSession session,
      ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
    return createColumnMetadata((TiDBColumnHandle) columnHandle);
  }

  @Override
  public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata,
      boolean ignoreExisting) {
    List<ColumnMetadata> columns = tableMetadata.getColumns();
    SchemaTableName table = tableMetadata.getTable();
    List<String> columnNames = columns.stream().map(ColumnMetadata::getName)
        .collect(toImmutableList());
    List<String> columnTypes = columns.stream()
        .map(column -> TypeHelpers.toSqlString(column.getType()))
        .collect(toImmutableList());
    getInternal().createTable(table.getSchemaName(), table.getTableName(), columnNames, columnTypes,
        ignoreExisting);
  }

  @Override
  public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle) {
    TiDBTableHandle handle = (TiDBTableHandle) tableHandle;
    String schemaName = handle.getSchemaName();
    String tableName = handle.getTableName();
    getInternal().dropTable(schemaName, tableName, true);
  }

  @Override
  public boolean schemaExists(ConnectorSession session, String schemaName) {
    return getInternal().databaseExists(schemaName);
  }

  @Override
  public void createSchema(ConnectorSession session, String schemaName,
      Map<String, Object> properties) {
    getInternal().createDatabase(schemaName, true);
  }

  @Override
  public void dropSchema(ConnectorSession session, String schemaName) {
    getInternal().dropDatabase(schemaName, true);
  }

  @Override
  public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle,
      SchemaTableName schemaTableName) {
    TiDBTableHandle handle = (TiDBTableHandle) tableHandle;
    getInternal()
        .renameTable(handle.getSchemaName(), schemaTableName.getSchemaName(),
            handle.getTableName(), schemaTableName.getTableName());
  }

  @Override
  public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle,
      ColumnMetadata column) {
    TiDBTableHandle handle = (TiDBTableHandle) tableHandle;
    getInternal().addColumn(handle.getSchemaName(), handle.getTableName(), column.getName(),
        TypeHelpers.toSqlString(column.getType()));
  }

  @Override
  public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle,
      ColumnHandle source, String target) {
    TiDBTableHandle handle = (TiDBTableHandle) tableHandle;
    TiDBColumnHandle columnHandle = (TiDBColumnHandle) source;
    getInternal()
        .renameColumn(handle.getSchemaName(), handle.getTableName(), columnHandle.getName(), target,
            TypeHelpers.toSqlString(columnHandle.getPrestoType()));
  }

  @Override
  public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle,
      ColumnHandle column) {
    TiDBTableHandle handle = (TiDBTableHandle) tableHandle;
    TiDBColumnHandle columnHandle = (TiDBColumnHandle) column;
    getInternal().dropColumn(handle.getSchemaName(), handle.getTableName(), columnHandle.getName());
  }
}
