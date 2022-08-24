/*
 * Copyright 2020 TiDB Project Authors.
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

package io.tidb.bigdata.tidb;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.tidb.bigdata.tidb.expression.Expression;
import io.tidb.bigdata.tidb.handle.ColumnHandleInternal;
import io.tidb.bigdata.tidb.key.Base64KeyRange;
import io.tidb.bigdata.tidb.meta.TiDAGRequest;
import io.tidb.bigdata.tidb.meta.TiDAGRequest.Builder;
import io.tidb.bigdata.tidb.meta.TiTableInfo;
import io.tidb.bigdata.tidb.operation.iterator.CoprocessorIterator;
import io.tidb.bigdata.tidb.row.Row;
import io.tidb.bigdata.tidb.types.DataType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.tikv.common.meta.TiTimestamp;

public final class RecordSetInternal {

  private final ClientSession session;
  private final List<SplitInternal> splits;
  private final List<Base64KeyRange> ranges;
  private final boolean queryHandle;
  private final List<ColumnHandleInternal> columnHandles;
  private final List<DataType> columnTypes;
  private final Supplier<RowHandleIterator> iteratorSupplier;

  public RecordSetInternal(
      ClientSession session,
      SplitInternal split,
      List<ColumnHandleInternal> columnHandles,
      Optional<Expression> expression,
      Optional<TiTimestamp> timestamp) {
    this(session, ImmutableList.of(split), columnHandles, expression, timestamp, Optional.empty());
  }

  public RecordSetInternal(
      ClientSession session,
      List<SplitInternal> splits,
      List<ColumnHandleInternal> columnHandles,
      Optional<Expression> expression,
      Optional<TiTimestamp> timestamp) {
    this(session, splits, columnHandles, expression, timestamp, Optional.empty());
  }

  public RecordSetInternal(
      ClientSession session,
      SplitInternal split,
      List<ColumnHandleInternal> columnHandles,
      Optional<Expression> expression,
      Optional<TiTimestamp> timestamp,
      Optional<Integer> limit) {
    this(session, ImmutableList.of(split), columnHandles, expression, timestamp, limit);
  }

  public RecordSetInternal(
      ClientSession session,
      List<SplitInternal> splits,
      List<ColumnHandleInternal> columnHandles,
      Optional<Expression> expression,
      Optional<TiTimestamp> timestamp,
      Optional<Integer> limit) {
    this(session, splits, columnHandles, expression, timestamp, limit, false);
  }

  public RecordSetInternal(
      ClientSession session,
      List<SplitInternal> splits,
      List<ColumnHandleInternal> columnHandles,
      Optional<Expression> expression,
      Optional<TiTimestamp> timestamp,
      Optional<Integer> limit,
      boolean queryHandle) {
    this.session = session;
    this.splits = splits;
    this.queryHandle = queryHandle;
    this.columnHandles = requireNonNull(columnHandles, "ColumnHandles can not be null");
    this.columnTypes =
        columnHandles.stream().map(ColumnHandleInternal::getType).collect(toImmutableList());
    checkSplits();
    this.ranges =
        splits.stream()
            .map(
                splitInternal ->
                    new Base64KeyRange(splitInternal.getStartKey(), splitInternal.getEndKey()))
            .collect(Collectors.toList());
    this.iteratorSupplier = () -> iterator(expression, timestamp, limit);
  }

  private void checkSplits() {
    Preconditions.checkArgument(
        splits != null && splits.size() >= 1, "Splits can not be empty or null");
    Preconditions.checkArgument(
        splits.stream().map(SplitInternal::getTimestamp).distinct().count() == 1,
        "Timestamp for splits must be equals");
    Preconditions.checkArgument(
        splits.stream().map(split -> split.getTable().getSchemaTableName()).distinct().count() == 1,
        "Table for splits must be equals");
  }

  public List<DataType> getColumnTypes() {
    return columnTypes;
  }

  public RecordCursorInternal cursor() {
    return new RecordCursorInternal(columnHandles, iteratorSupplier.get());
  }

  private TiDAGRequest.Builder buildRequest(
      List<String> columns,
      Optional<Expression> expression,
      Optional<TiTimestamp> timestamp,
      Optional<Integer> limit) {
    SplitInternal split = splits.get(0);
    TiDAGRequest.Builder request = session.request(split.getTable(), columns);
    limit.ifPresent(request::setLimit);
    expression.ifPresent(request::addFilter);
    request.setStartTs(split.getTimestamp());
    timestamp.ifPresent(request::setStartTs);
    return request;
  }

  private RowHandleIterator iterator(
      Optional<Expression> expression, Optional<TiTimestamp> timestamp, Optional<Integer> limit) {
    SplitInternal split = splits.get(0);
    List<String> columns =
        columnHandles.stream().map(ColumnHandleInternal::getName).collect(toImmutableList());
    if (!queryHandle) {
      Builder request = buildRequest(columns, expression, timestamp, limit);
      return RowHandleIterator.createWrap(session.iterate(request, ranges));
    } else {
      TiTableInfo tiTableInfo =
          session.getTableMust(
              split.getTable().getSchemaName(), split.getTable().getTableName(), true);
      List<String> queryColumns = new ArrayList<>(columns);
      if (tiTableInfo.getColumn(ClientSession.TIDB_ROW_ID_COLUMN_NAME) == null) {
        // Add primary key to query
        List<String> primaryKeyColumns =
            session.getPrimaryKeyColumns(
                split.getTable().getSchemaName(), split.getTable().getTableName());
        for (String column : primaryKeyColumns) {
          if (!queryColumns.contains(column)) {
            queryColumns.add(column);
          }
        }
      } else {
        // Add row id to query
        if (!queryColumns.contains(ClientSession.TIDB_ROW_ID_COLUMN_NAME)) {
          queryColumns.add(ClientSession.TIDB_ROW_ID_COLUMN_NAME);
        }
      }
      Builder request = buildRequest(queryColumns, expression, timestamp, limit);
      CoprocessorIterator<Row> iterate = session.iterate(request, ranges);
      return RowHandleIterator.create(tiTableInfo, columns, queryColumns, iterate);
    }
  }
}
