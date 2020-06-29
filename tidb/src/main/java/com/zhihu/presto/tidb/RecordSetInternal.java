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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.operation.iterator.CoprocessIterator;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.DataType;
import java.util.List;
import java.util.Optional;

public final class RecordSetInternal {

  private final List<ColumnHandleInternal> columnHandles;
  private final List<DataType> columnTypes;
  private final CoprocessIterator<Row> iterator;

  public RecordSetInternal(ClientSession session, SplitInternal split,
      List<ColumnHandleInternal> columnHandles, Optional<Expression> expression) {
    requireNonNull(split, "split is null");
    this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
    this.columnTypes = columnHandles.stream().map(ColumnHandleInternal::getType)
        .collect(toImmutableList());
    List<String> columns = columnHandles.stream().map(ColumnHandleInternal::getName)
        .collect(toImmutableList());

    TiDAGRequest.Builder request = session.request(split.getTable(), columns);
    expression.ifPresent(e -> request.addFilter(e));
    iterator = session.iterate(request, new Base64KeyRange(split.getStartKey(), split.getEndKey()));
  }

  public List<DataType> getColumnTypes() {
    return columnTypes;
  }

  public RecordCursorInternal cursor() {
    return new RecordCursorInternal(columnHandles, iterator);
  }
}
