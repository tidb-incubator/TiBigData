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

package com.zhihu.tibigdata.prestodb.tidb;

import static com.zhihu.tibigdata.prestodb.tidb.JdbcErrorCode.JDBC_ERROR;
import static com.zhihu.tibigdata.prestodb.tidb.TiDBConfig.SESSION_WRITE_MODE;
import static com.zhihu.tibigdata.tidb.TiDBWriteMode.fromString;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PageSinkProperties;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.zhihu.tibigdata.tidb.TiDBWriteMode;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import javax.inject.Inject;

public class TiDBPageSinkProvider implements ConnectorPageSinkProvider {

  @Inject
  private TiDBMetadata metadata;

  @Override
  public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle,
      ConnectorSession session, ConnectorOutputTableHandle outputTableHandle,
      PageSinkProperties pageSinkProperties) {
    return createTiDBPageSink(session, (TiDBTableHandle) outputTableHandle);
  }

  @Override
  public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle,
      ConnectorSession session, ConnectorInsertTableHandle insertTableHandle,
      PageSinkProperties pageSinkProperties) {
    return createTiDBPageSink(session, (TiDBTableHandle) insertTableHandle);
  }

  private TiDBPageSink createTiDBPageSink(ConnectorSession session,
      TiDBTableHandle tiDBTableHandle) {
    final String schemaName = tiDBTableHandle.getSchemaName();
    final String tableName = tiDBTableHandle.getTableName();
    final List<ColumnMetadata> columns = metadata.getTableMetadata(null, tiDBTableHandle)
        .getColumns();
    final List<String> columnNames = columns.stream().map(ColumnMetadata::getName)
        .collect(ImmutableList.toImmutableList());
    final List<Type> columnTypes = columns.stream().map(ColumnMetadata::getType)
        .collect(ImmutableList.toImmutableList());
    TiDBWriteMode writeMode = fromString(session.getProperty(SESSION_WRITE_MODE, String.class));
    Connection connection;
    try {
      connection = metadata.getInternal().getJdbcConnection();
    } catch (SQLException e) {
      throw new PrestoException(JDBC_ERROR, e);
    }
    return new TiDBPageSink(schemaName, tableName, columnNames, columnTypes, writeMode, connection);
  }
}
