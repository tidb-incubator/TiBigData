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
import static com.zhihu.tibigdata.prestosql.tidb.JdbcErrorCode.JDBC_NON_TRANSIENT_ERROR;
import static com.zhihu.tibigdata.tidb.SqlUtils.getInsertSql;
import static com.zhihu.tibigdata.tidb.SqlUtils.getUpsertSql;
import static com.zhihu.tibigdata.tidb.TiDBWriteMode.UPSERT;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.Chars.isCharType;
import static io.prestosql.spi.type.Decimals.readBigDecimal;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.joda.time.chrono.ISOChronology.getInstanceUTC;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import com.zhihu.tibigdata.tidb.TiDBWriteMode;
import io.airlift.slice.Slice;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Type;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiDBPageSink implements ConnectorPageSink {

  static final Logger LOG = LoggerFactory.getLogger(TiDBPageSink.class);

  private final String schemaName;

  private final String tableName;

  private final List<String> columnNames;

  private final List<Type> columnTypes;

  private final TiDBWriteMode writeMode;

  private final Connection connection;

  private final PreparedStatement statement;

  private int batchSize;

  public TiDBPageSink(String schemaName, String tableName, List<String> columnNames,
      List<Type> columnTypes, TiDBWriteMode writeMode, Connection connection) {
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
    this.writeMode = writeMode;
    this.connection = connection;
    try {
      connection.setAutoCommit(false);
      statement = connection.prepareStatement(
          writeMode == UPSERT ? getUpsertSql(schemaName, tableName, columnNames)
              : getInsertSql(schemaName, tableName, columnNames));
    } catch (SQLException e) {
      closeWithSuppression(connection, e);
      throw new PrestoException(JDBC_ERROR, e);
    }
  }

  @Override
  public CompletableFuture<?> appendPage(Page page) {
    try {
      for (int position = 0; position < page.getPositionCount(); position++) {
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
          appendColumn(page, position, channel);
        }

        statement.addBatch();
        batchSize++;

        if (batchSize >= 1000) {
          statement.executeBatch();
          connection.commit();
          connection.setAutoCommit(false);
          batchSize = 0;
        }
      }
    } catch (SQLException e) {
      throw new PrestoException(JDBC_ERROR, e);
    }
    return NOT_BLOCKED;
  }

  private void appendColumn(Page page, int position, int channel) throws SQLException {
    Block block = page.getBlock(channel);
    int parameter = channel + 1;

    if (block.isNull(position)) {
      statement.setObject(parameter, null);
      return;
    }

    Type type = columnTypes.get(channel);
    switch (type.getDisplayName()) {
      case "boolean":
        statement.setBoolean(parameter, type.getBoolean(block, position));
        break;
      case "tinyint":
        statement.setByte(parameter, SignedBytes.checkedCast(type.getLong(block, position)));
        break;
      case "smallint":
        statement.setShort(parameter, Shorts.checkedCast(type.getLong(block, position)));
        break;
      case "integer":
        statement.setInt(parameter, toIntExact(type.getLong(block, position)));
        break;
      case "bigint":
        statement.setLong(parameter, type.getLong(block, position));
        break;
      case "real":
        statement.setFloat(parameter, intBitsToFloat(toIntExact(type.getLong(block, position))));
        break;
      case "double":
        statement.setDouble(parameter, type.getDouble(block, position));
        break;
      case "date":
        // convert to midnight in default time zone
        long utcMillis = DAYS.toMillis(type.getLong(block, position));
        long localMillis = getInstanceUTC().getZone()
            .getMillisKeepLocal(DateTimeZone.getDefault(), utcMillis);
        statement.setDate(parameter, new Date(localMillis));
        break;
      case "time":
        statement.setTime(parameter, new Time(type.getLong(block, position)));
        break;
      case "timestamp":
        statement.setTimestamp(parameter, new Timestamp(type.getLong(block, position)));
        break;
      case "varbinary":
        statement.setBytes(parameter, type.getSlice(block, position).getBytes());
        break;
      default:
        if (type instanceof DecimalType) {
          statement.setBigDecimal(parameter, readBigDecimal((DecimalType) type, block, position));
        } else if (isVarcharType(type) || isCharType(type)) {
          statement.setString(parameter, type.getSlice(block, position).toStringUtf8());
        } else {
          throw new PrestoException(NOT_SUPPORTED,
              "Unsupported column type: " + type.getDisplayName());
        }
    }
  }

  @Override
  public CompletableFuture<Collection<Slice>> finish() {
    // commit and close
    try (Connection connection = this.connection;
        PreparedStatement statement = this.statement) {
      if (batchSize > 0) {
        statement.executeBatch();
        connection.commit();
      }
    } catch (SQLNonTransientException e) {
      throw new PrestoException(JDBC_NON_TRANSIENT_ERROR, e);
    } catch (SQLException e) {
      throw new PrestoException(JDBC_ERROR, e);
    }
    // the committer does not need any additional info
    return completedFuture(ImmutableList.of());
  }

  @Override
  public void abort() {
    // rollback and close
    try (Connection connection = this.connection;
        PreparedStatement statement = this.statement) {
      connection.rollback();
    } catch (SQLException e) {
      // Exceptions happened during abort do not cause any real damage so ignore them
      LOG.debug("SQLException when abort", e);
    }
  }

  @SuppressWarnings("ObjectEquality")
  private static void closeWithSuppression(Connection connection, Throwable throwable) {
    try {
      connection.close();
    } catch (Throwable t) {
      // Self-suppression not permitted
      if (throwable != t) {
        throwable.addSuppressed(t);
      }
    }
  }
}