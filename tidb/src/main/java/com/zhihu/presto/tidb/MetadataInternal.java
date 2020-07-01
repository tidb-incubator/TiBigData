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

package com.zhihu.presto.tidb;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class MetadataInternal {

  private final String connectorId;
  private final ClientSession session;

  public MetadataInternal(String connectorId, ClientSession session) {
    this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    this.session = requireNonNull(session, "session is null");
  }

  public List<String> listSchemaNames() {
    return session.getSchemaNames();
  }

  public Optional<TableHandleInternal> getTableHandle(String schemaName, String tableName) {
    return session.getTable(schemaName, tableName)
        .map(t -> new TableHandleInternal(connectorId, schemaName, tableName));
  }

  public Map<String, List<String>> listTables(Optional<String> schemaName) {
    return session.listTables(schemaName);
  }

  public Optional<List<ColumnHandleInternal>> getColumnHandles(TableHandleInternal tableHandle) {
    return session.getTableColumns(tableHandle);
  }

  public Optional<List<ColumnHandleInternal>> getColumnHandles(String schemaName,
      String tableName) {
    return session.getTableColumns(schemaName, tableName);
  }

  public String getConnectorId() {
    return connectorId;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("connectorId", connectorId)
        .add("session", session)
        .toString();
  }
}
