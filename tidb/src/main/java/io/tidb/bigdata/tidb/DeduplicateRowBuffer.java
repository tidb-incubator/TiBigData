/*
 * Copyright 2022 TiDB Project Authors.
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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.tidb.bigdata.tidb.meta.TiIndexColumn;
import io.tidb.bigdata.tidb.meta.TiIndexInfo;
import io.tidb.bigdata.tidb.meta.TiTableInfo;
import io.tidb.bigdata.tidb.row.Row;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class DeduplicateRowBuffer extends RowBuffer {

  private final List<TiIndexInfo> uniqueIndexs;
  private final Row2ukUtil row2ukUtil;

  public DeduplicateRowBuffer(
      TiTableInfo tiTableInfo, boolean ignoreAutoincrementColumn, int bufferSize) {
    // just use set to accelerate remove
    super(bufferSize, new LinkedHashSet<>());
    uniqueIndexs = SqlUtils.getUniqueIndexes(tiTableInfo, ignoreAutoincrementColumn);
    row2ukUtil = new Row2ukUtil();
  }

  @Override
  public boolean add(Row row) {
    if (isFull()) {
      throw new IllegalStateException("Row buffer is full!");
    }
    if (uniqueIndexs.size() == 0) {
      rows.add(row);
      return true;
    }
    boolean conflict = true;
    for (TiIndexInfo indexInfo : uniqueIndexs) {
      // get ukWithValue
      List<TiIndexColumn> indexColumns = indexInfo.getIndexColumns();
      List<Object> indexValue =
          Collections.unmodifiableList(
              indexColumns.stream()
                  .map(i -> row.get(i.getOffset(), null))
                  .collect(Collectors.toList()));
      UkWithValue ukWithValue = new UkWithValue(indexInfo, indexValue);
      // judge if conflict with old row
      if (row2ukUtil.ukWithValueExist(ukWithValue)) {
        conflict = false;
        // get old row that is conflict
        Row oldRow = row2ukUtil.getRow(ukWithValue);
        // remove the old row from row buffer
        rows.remove(oldRow);
        // remove the old row's relationship
        row2ukUtil.removeRow(oldRow);
      }
      // add the new row's relationship
      row2ukUtil.add(row, ukWithValue);
    }
    // add new row
    rows.add(row);

    return conflict;
  }

  @Override
  public void clear() {
    super.clear();
    row2ukUtil.clear();
  }

  public static class Row2ukUtil {
    Multimap<Row, UkWithValue> row2Uk = ArrayListMultimap.create();
    Map<UkWithValue, Row> uk2Row = new HashMap<>();

    public boolean ukWithValueExist(UkWithValue ukWithValue) {
      return uk2Row.containsKey(ukWithValue);
    }

    public Row getRow(UkWithValue ukWithValue) {
      return uk2Row.get(ukWithValue);
    }

    public void removeRow(Row row) {
      Collection<UkWithValue> ukWithValues = row2Uk.get(row);
      ukWithValues.forEach(e -> uk2Row.remove(e));
      row2Uk.removeAll(row);
    }

    public void add(Row row, UkWithValue ukWithValue) {
      row2Uk.put(row, ukWithValue);
      uk2Row.put(ukWithValue, row);
    }

    public void clear() {
      row2Uk.clear();
      uk2Row.clear();
    }
  }

  static class UkWithValue {
    TiIndexInfo uniqueIndex;
    // the values will be unmodifiableList
    List<Object> values;

    public UkWithValue(TiIndexInfo uniqueIndex, List<Object> values) {
      this.uniqueIndex = uniqueIndex;
      this.values = values;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      UkWithValue that = (UkWithValue) o;
      return Objects.equals(uniqueIndex, that.uniqueIndex) && Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
      return Objects.hash(uniqueIndex, values);
    }
  }
}
