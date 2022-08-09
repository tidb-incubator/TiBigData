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

package io.tidb.bigdata.trino.tidb;

import static io.tidb.bigdata.tidb.TiDBWriteMode.fromString;
import static io.tidb.bigdata.trino.tidb.JdbcErrorCode.JDBC_ERROR;
import static io.tidb.bigdata.trino.tidb.TiDBConfig.SESSION_WRITE_MODE;

import com.google.common.collect.ImmutableList;
import io.tidb.bigdata.tidb.TiDBWriteMode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import javax.inject.Inject;

public class TiDBPageSinkProvider implements ConnectorPageSinkProvider {

  @Inject private TiDBMetadata metadata;

  @Override
  public ConnectorPageSink createPageSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorOutputTableHandle outputTableHandle) {
    return createTiDBPageSink(session, (TiDBTableHandle) outputTableHandle);
  }

  @Override
  public ConnectorPageSink createPageSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorInsertTableHandle insertTableHandle) {
    return createTiDBPageSink(session, (TiDBTableHandle) insertTableHandle);
  }

  private TiDBPageSink createTiDBPageSink(
      ConnectorSession session, TiDBTableHandle tiDBTableHandle) {
    final String schemaName = tiDBTableHandle.getSchemaName();
    final String tableName = tiDBTableHandle.getTableName();
    final List<ColumnMetadata> columns =
        metadata.getTableMetadata(null, tiDBTableHandle).getColumns();
    final List<String> columnNames =
        columns.stream().map(ColumnMetadata::getName).collect(ImmutableList.toImmutableList());
    final List<Type> columnTypes =
        columns.stream().map(ColumnMetadata::getType).collect(ImmutableList.toImmutableList());
    TiDBWriteMode writeMode = fromString(session.getProperty(SESSION_WRITE_MODE, String.class));
    Connection connection;
    try {
      connection = metadata.getInternal().getJdbcConnection();
    } catch (SQLException e) {
      throw new TrinoException(JDBC_ERROR, e);
    }
    return new TiDBPageSink(schemaName, tableName, columnNames, columnTypes, writeMode, connection);
  }
}
