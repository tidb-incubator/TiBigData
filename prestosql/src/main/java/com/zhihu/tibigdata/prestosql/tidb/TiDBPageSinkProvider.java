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

import static com.zhihu.tibigdata.prestosql.tidb.JdbcErrorCode.JDBC_ERROR;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.type.Type;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import javax.inject.Inject;

public class TiDBPageSinkProvider implements ConnectorPageSinkProvider {

  @Inject
  private TiDBMetadata metadata;

  @Override
  public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle,
      ConnectorSession session, ConnectorOutputTableHandle outputTableHandle) {
    return createTiDBPageSink((TiDBTableHandle) outputTableHandle);
  }

  @Override
  public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle,
      ConnectorSession session, ConnectorInsertTableHandle insertTableHandle) {
    return createTiDBPageSink((TiDBTableHandle) insertTableHandle);
  }

  private TiDBPageSink createTiDBPageSink(TiDBTableHandle tiDBTableHandle) {
    final String schemaName = tiDBTableHandle.getSchemaName();
    final String tableName = tiDBTableHandle.getTableName();
    final List<ColumnMetadata> columns = metadata.getTableMetadata(null, tiDBTableHandle)
        .getColumns();
    final List<String> columnNames = columns.stream().map(ColumnMetadata::getName)
        .collect(ImmutableList.toImmutableList());
    final List<Type> columnTypes = columns.stream().map(ColumnMetadata::getType)
        .collect(ImmutableList.toImmutableList());
    Connection connection;
    try {
      connection = metadata.getInternal().getJdbcConnection();
    } catch (SQLException e) {
      throw new PrestoException(JDBC_ERROR, e);
    }
    return new TiDBPageSink(schemaName, tableName, columnNames, columnTypes, connection);
  }
}
