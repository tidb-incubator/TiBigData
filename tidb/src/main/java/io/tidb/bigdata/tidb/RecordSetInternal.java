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

import com.google.common.base.Preconditions;
import io.tidb.bigdata.tidb.expression.Expression;
import io.tidb.bigdata.tidb.handle.ColumnHandleInternal;
import io.tidb.bigdata.tidb.key.Base64KeyRange;
import io.tidb.bigdata.tidb.meta.TiDAGRequest;
import io.tidb.bigdata.tidb.meta.TiTableInfo;
import io.tidb.bigdata.tidb.operation.iterator.CoprocessorIterator;
import io.tidb.bigdata.tidb.row.Row;
import io.tidb.bigdata.tidb.types.DataType;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.tikv.common.meta.TiTimestamp;

public final class RecordSetInternal {

  private final ClientSession session;
  private final List<SplitInternal> splits;
  private final List<ColumnHandleInternal> columnHandles;
  private final Expression expression;
  private final TiTimestamp timestamp;
  private final Integer limit;
  private final boolean queryHandle;

  private final List<DataType> columnTypes;

  private RecordSetInternal(
      @Nonnull ClientSession session,
      @Nonnull List<SplitInternal> splits,
      @Nonnull List<ColumnHandleInternal> columnHandles,
      Expression expression,
      TiTimestamp timestamp,
      Integer limit,
      boolean queryHandle) {
    this.session = session;
    this.splits = splits;
    this.columnHandles = columnHandles;
    this.expression = expression;
    this.timestamp = timestamp;
    this.limit = limit;
    this.queryHandle = queryHandle;
    this.columnTypes =
        columnHandles.stream().map(ColumnHandleInternal::getType).collect(toImmutableList());
    checkSplits();
  }

  private void checkSplits() {
    Preconditions.checkArgument(splits.size() >= 1, "Splits can not be empty");
    Preconditions.checkArgument(
        splits.stream().map(SplitInternal::getTimestamp).distinct().count() == 1,
        "Timestamp for splits must be equals");
    Preconditions.checkArgument(
        splits.stream().map(split -> split.getTable().getSchemaTableName()).distinct().count() == 1,
        "Table for splits must be equals");
  }

  private TiDAGRequest.Builder buildRequest(List<String> columns) {
    SplitInternal split = splits.get(0);
    TiDAGRequest.Builder request = session.request(split.getTable(), columns);
    if (limit != null) {
      request.setLimit(limit);
    }
    if (expression != null) {
      request.addFilter(expression);
    }
    if (timestamp != null) {
      request.setStartTs(timestamp);
    } else {
      request.setStartTs(split.getTimestamp());
    }
    return request;
  }

  public RecordCursorInternal cursor() {
    return new RecordCursorInternal(columnHandles, iterator());
  }

  private RowHandleIterator iterator() {
    SplitInternal split = splits.get(0);
    List<String> columns =
        columnHandles.stream().map(ColumnHandleInternal::getName).collect(toImmutableList());
    List<Base64KeyRange> ranges =
        splits.stream()
            .map(
                splitInternal ->
                    new Base64KeyRange(splitInternal.getStartKey(), splitInternal.getEndKey()))
            .collect(Collectors.toList());

    // Whether generate handle for each tidb row, there are two cases:
    // 1. _tidb_rowid in table columns, handle is the value of _tidb_rowid;
    // 2. table is pk handle or common handle, we will recalculate handle by primary key.
    // If query handle is true, we should add primary key columns or _tidb_rowid into query columns
    // because column pruning may remove these columns.
    if (!queryHandle) {
      TiDAGRequest.Builder request = buildRequest(columns);
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
      TiDAGRequest.Builder request = buildRequest(queryColumns);
      CoprocessorIterator<Row> iterate = session.iterate(request, ranges);
      return RowHandleIterator.create(tiTableInfo, columns, queryColumns, iterate);
    }
  }

  public List<DataType> getColumnTypes() {
    return columnTypes;
  }

  public static Builder builder(
      ClientSession session, List<SplitInternal> splits, List<ColumnHandleInternal> columnHandles) {
    return new Builder(session, splits, columnHandles);
  }

  public static final class Builder {

    private final ClientSession session;
    private final List<SplitInternal> splits;
    private List<ColumnHandleInternal> columnHandles;
    private Expression expression;
    private TiTimestamp timestamp;
    private Integer limit;
    private boolean queryHandle;

    private Builder(
        @Nonnull ClientSession session,
        @Nonnull List<SplitInternal> splits,
        @Nonnull List<ColumnHandleInternal> columnHandles) {
      this.session = session;
      this.splits = splits;
      this.columnHandles = columnHandles;
    }

    public Builder withExpression(Expression expression) {
      this.expression = expression;
      return this;
    }

    public Builder withTimestamp(TiTimestamp timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public Builder withLimit(Integer limit) {
      this.limit = limit;
      return this;
    }

    public Builder withQueryHandle(boolean queryHandle) {
      this.queryHandle = queryHandle;
      return this;
    }

    public RecordSetInternal build() {
      return new RecordSetInternal(
          session, splits, columnHandles, expression, timestamp, limit, queryHandle);
    }
  }
}
