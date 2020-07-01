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

package com.zhihu.prestosql.tidb;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.zhihu.prestosql.tidb.TypeHelpers.getHelper;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

import com.google.common.collect.ImmutableMap;
import com.zhihu.presto.tidb.ColumnHandleInternal;
import com.zhihu.presto.tidb.MetadataInternal;
import com.zhihu.presto.tidb.Wrapper;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    return getTableMetadata(
        new SchemaTableName(tableHandle.getSchemaName(), tableHandle.getTableName()));
  }

  private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTable) {
    return new ConnectorTableMetadata(schemaTable,
        getTableMetadataStream(schemaTable).collect(toImmutableList()));
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
}
