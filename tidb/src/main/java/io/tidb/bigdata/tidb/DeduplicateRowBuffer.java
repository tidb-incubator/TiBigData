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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class DeduplicateRowBuffer extends RowBuffer {

  private final List<TiIndexInfo> uniqueIndexes;
  private final RowUKBiMap rowUKBiMap;

  public DeduplicateRowBuffer(
      TiTableInfo tiTableInfo, boolean ignoreAutoincrementColumn, int bufferSize) {
    // just use set to accelerate remove
    super(bufferSize, new LinkedHashSet<>());
    uniqueIndexes = SqlUtils.getUniqueIndexes(tiTableInfo, ignoreAutoincrementColumn);
    rowUKBiMap = new RowUKBiMap();
  }

  // return true if there is no conflict
  @Override
  public boolean add(Row row) {
    if (isFull()) {
      throw new IllegalStateException("Row buffer is full!");
    }
    if (uniqueIndexes.size() == 0) {
      rows.add(row);
      return true;
    }
    boolean conflict = false;
    for (TiIndexInfo indexInfo : uniqueIndexes) {
      // get uniqueKeyColumns
      List<TiIndexColumn> indexColumns = indexInfo.getIndexColumns();
      List<Object> indexValue =
          Collections.unmodifiableList(
              indexColumns.stream()
                  .map(i -> row.get(i.getOffset(), null))
                  .collect(Collectors.toList()));
      UniqueKeyColumns uniqueKeyColumns = new UniqueKeyColumns(indexInfo, indexValue);
      // get old row
      Row oldRow = rowUKBiMap.getRow(uniqueKeyColumns);
      if (oldRow != null) {
        conflict = true;
        // remove the old row from row buffer
        rows.remove(oldRow);
        // remove the old row's relationship
        rowUKBiMap.removeRow(oldRow);
      }
      // add the new row's uniqueKeyColumns
      rowUKBiMap.add(row, uniqueKeyColumns);
    }
    // add new row
    rows.add(row);

    return !conflict;
  }

  @Override
  public void clear() {
    super.clear();
    rowUKBiMap.clear();
  }

  private static class RowUKBiMap {
    private final Multimap<Row, UniqueKeyColumns> row2Uk = ArrayListMultimap.create();
    private final Map<UniqueKeyColumns, Row> uk2Row = new HashMap<>();

    public Row getRow(UniqueKeyColumns uniqueKeyColumns) {
      return uk2Row.get(uniqueKeyColumns);
    }

    public void removeRow(Row row) {
      Collection<UniqueKeyColumns> uniqueKeyColumns = row2Uk.get(row);
      uniqueKeyColumns.forEach(uk2Row::remove);
      row2Uk.removeAll(row);
    }

    public void add(Row row, UniqueKeyColumns uniqueKeyColumns) {
      row2Uk.put(row, uniqueKeyColumns);
      uk2Row.put(uniqueKeyColumns, row);
    }

    public void clear() {
      row2Uk.clear();
      uk2Row.clear();
    }
  }

  private static class UniqueKeyColumns {
    private final TiIndexInfo uniqueIndex;
    // the values will be unmodifiableList
    private final List<Object> values;

    public UniqueKeyColumns(TiIndexInfo uniqueIndex, List<Object> values) {
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
      UniqueKeyColumns that = (UniqueKeyColumns) o;
      return Objects.equals(uniqueIndex, that.uniqueIndex) && Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
      return Objects.hash(uniqueIndex, values);
    }
  }
}
