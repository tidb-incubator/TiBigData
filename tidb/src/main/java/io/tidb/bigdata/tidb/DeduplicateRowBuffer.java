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

import io.tidb.bigdata.tidb.meta.TiIndexColumn;
import io.tidb.bigdata.tidb.meta.TiIndexInfo;
import io.tidb.bigdata.tidb.meta.TiTableInfo;
import io.tidb.bigdata.tidb.row.Row;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DeduplicateRowBuffer extends RowBuffer {

  private final List<UniqueIndexAndValues> uniqueIndexValues;
  // row -> uk's values
  private final Map<Row, List<List<Object>>> row2Values;

  public DeduplicateRowBuffer(
      TiTableInfo tiTableInfo, boolean ignoreAutoincrementColumn, int bufferSize) {
    // just use set to accelerate remove
    super(bufferSize, new LinkedHashSet<>());
    List<TiIndexInfo> uniqueIndexes =
        SqlUtils.getUniqueIndexes(tiTableInfo, ignoreAutoincrementColumn);
    this.uniqueIndexValues = new ArrayList<>(uniqueIndexes.size());
    for (TiIndexInfo uniqueIndex : uniqueIndexes) {
      List<Integer> columnIndex =
          uniqueIndex.getIndexColumns().stream()
              .map(TiIndexColumn::getOffset)
              .collect(Collectors.toList());
      uniqueIndexValues.add(new UniqueIndexAndValues(columnIndex, new HashMap<>()));
    }
    row2Values = new HashMap<>();
  }

  @Override
  public boolean add(Row row) {
    if (isFull()) {
      throw new IllegalStateException("Row buffer is full!");
    }
    if (uniqueIndexValues.size() == 0) {
      rows.add(row);
      return true;
    }
    List<List<Object>> values = new ArrayList<>();
    boolean conflict = true;
    for (UniqueIndexAndValues uniqueIndexAndValues : uniqueIndexValues) {
      List<Object> indexValue =
          Collections.unmodifiableList(
              uniqueIndexAndValues.pos.stream()
                  .map(i -> row.get(i, null))
                  .collect(Collectors.toList()));
      if (uniqueIndexAndValues.isConflict(indexValue)) {
        conflict = false;
        // delete the old row
        Row deleteRow = uniqueIndexAndValues.getRow(indexValue);
        rows.remove(deleteRow);
        // delete the old index values
        List<List<Object>> oldValues = row2Values.get(deleteRow);
        for (int i = 0; i < uniqueIndexValues.size(); i++) {
          uniqueIndexValues.get(i).deleteValues2Row(oldValues.get(i));
        }
        // delete row -> uk's values
        row2Values.remove(deleteRow);
      }
      // build the index values for the new row
      values.add(indexValue);
    }
    // add the new row
    rows.add(row);
    // add the new index values
    for (int i = 0; i < uniqueIndexValues.size(); i++) {
      uniqueIndexValues.get(i).addValues2Row(values.get(i), row);
    }
    // add row -> uk's values
    row2Values.put(row, values);
    return conflict;
  }

  @Override
  public void clear() {
    super.clear();
    for (UniqueIndexAndValues uniqueIndexValue : uniqueIndexValues) {
      uniqueIndexValue.clear();
    }
    row2Values.clear();
  }

  // the uniqueIndex and all its values, every values determines a row
  static class UniqueIndexAndValues {
    List<Integer> pos;
    // List will be unmodifiableList, so take it easy to use it be the key of map
    Map<List<Object>, Row> values2Row;

    public UniqueIndexAndValues(List<Integer> pos, Map<List<Object>, Row> values2Row) {
      this.pos = pos;
      this.values2Row = values2Row;
    }

    public void clear() {
      values2Row = new HashMap<>();
    }

    public void deleteValues2Row(List<Object> values) {
      values2Row.remove(values);
    }

    public void addValues2Row(List<Object> values, Row row) {
      values2Row.put(values, row);
    }

    public Row getRow(List<Object> values) {
      return values2Row.get(values);
    }

    public Boolean isConflict(List<Object> values) {
      return values2Row.containsKey(values);
    }
  }
}
