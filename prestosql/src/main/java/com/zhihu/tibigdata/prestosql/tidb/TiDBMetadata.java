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

package com.zhihu.tibigdata.prestosql.tidb;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.zhihu.tibigdata.prestosql.tidb.TiDBConfig.PRIMARY_KEY;
import static com.zhihu.tibigdata.prestosql.tidb.TiDBConfig.UNIQUE_KEY;
import static com.zhihu.tibigdata.prestosql.tidb.TypeHelpers.getHelper;
import static java.lang.String.join;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

import com.google.common.collect.ImmutableMap;
import com.zhihu.tibigdata.tidb.ColumnHandleInternal;
import com.zhihu.tibigdata.tidb.MetadataInternal;
import com.zhihu.tibigdata.tidb.Wrapper;
import io.airlift.slice.Slice;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.statistics.ComputedStatistics;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
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
  public boolean usesLegacyTableLayouts() {
    return false;
  }

  @Override
  public ConnectorTableMetadata getTableMetadata(ConnectorSession session,
      ConnectorTableHandle table) {
    TiDBTableHandle tableHandle = (TiDBTableHandle) table;
    checkArgument(tableHandle.getConnectorId().equals(getInternal().getConnectorId()),
        "tableHandle is not for this connector");
    return getTableMetadata(tableHandle.getSchemaName(), tableHandle.getTableName());
  }

  private ConnectorTableMetadata getTableMetadata(String schemaName, String tableName) {
    return new ConnectorTableMetadata(new SchemaTableName(schemaName, tableName),
        getTableMetadataStream(schemaName, tableName).collect(toImmutableList()),
        ImmutableMap.of(
            PRIMARY_KEY, join(",", getInternal().getPrimaryKeyColumns(schemaName, tableName)),
            UNIQUE_KEY, join(",", getInternal().getUniqueKeyColumns(schemaName, tableName))));
  }

  @Override
  public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
    return getInternal().listTables(schemaName).entrySet().stream().flatMap(entry -> {
      String schema = entry.getKey();
      return entry.getValue().stream().map(table -> new SchemaTableName(schema, table));
    }).collect(toImmutableList());
  }

  private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix) {
    List<SchemaTableName> tables = listTables(session, prefix.getSchema());
    return prefix.getTable().map(tablePrefix -> (List<SchemaTableName>) tables.stream()
        .filter(t -> t.getTableName().startsWith(tablePrefix)).collect(toImmutableList()))
        .orElse(tables);
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
  public ConnectorTableProperties getTableProperties(ConnectorSession session,
      ConnectorTableHandle table) {
    return new ConnectorTableProperties();
  }

  @Override
  public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata,
      boolean ignoreExisting) {
    List<ColumnMetadata> columns = tableMetadata.getColumns();
    SchemaTableName table = tableMetadata.getTable();
    String schemaName = table.getSchemaName();
    String tableName = table.getTableName();
    List<String> columnNames = columns.stream().map(ColumnMetadata::getName)
        .collect(toImmutableList());
    List<String> columnTypes = columns.stream()
        .map(column -> TypeHelpers.toSqlString(column.getType()))
        .collect(toImmutableList());
    List<String> primaryKeyColumns = Arrays
        .stream(tableMetadata.getProperties().get(PRIMARY_KEY).toString().split(","))
        .filter(s -> !s.isEmpty()).collect(Collectors.toList());
    checkArgument(columnNames.containsAll(primaryKeyColumns),
        "column names does not contain all primary key columns");
    List<String> uniqueKeyColumns = Arrays
        .stream(tableMetadata.getProperties().get(UNIQUE_KEY).toString().split(","))
        .filter(s -> !s.isEmpty()).collect(Collectors.toList());
    checkArgument(columnNames.containsAll(uniqueKeyColumns),
        "column names does not contain all unique key columns");
    getInternal().createTable(schemaName, tableName, columnNames, columnTypes, primaryKeyColumns,
        uniqueKeyColumns, ignoreExisting);
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
      Map<String, Object> properties, PrestoPrincipal owner) {
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

  @Override
  public ConnectorInsertTableHandle beginInsert(ConnectorSession session,
      ConnectorTableHandle tableHandle, List<ColumnHandle> columns) {
    return (TiDBTableHandle) tableHandle;
  }

  @Override
  public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session,
      ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments,
      Collection<ComputedStatistics> computedStatistics) {
    return Optional.empty();
  }
}
