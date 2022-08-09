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

package io.tidb.bigdata.tidb.buffer;

import io.tidb.bigdata.tidb.meta.TiTableInfo;
import io.tidb.bigdata.tidb.row.Row;
import java.util.ArrayList;
import java.util.Collection;

public abstract class RowBuffer {

  protected final int bufferSize;
  protected Collection<Row> rows;

  protected RowBuffer(int bufferSize, Collection<Row> rows) {
    this.bufferSize = bufferSize;
    this.rows = rows;
  }

  protected RowBuffer(int bufferSize) {
    this.bufferSize = bufferSize;
    this.rows = new ArrayList<>();
  }

  public abstract boolean add(Row row);

  public int size() {
    return rows.size();
  }

  public int addAll(Collection<Row> collection) {
    return (int) collection.stream().map(this::add).filter(b -> b).count();
  }

  public Collection<Row> getRows() {
    return rows;
  }

  public void clear() {
    // clear is better than new
    this.rows.clear();
  }

  public boolean isFull() {
    return rows.size() == bufferSize;
  }

  public static RowBuffer createDefault(int bufferSize) {
    return new DefaultRowBuffer(bufferSize);
  }

  public static RowBuffer createDeduplicateRowBuffer(
      TiTableInfo tiTableInfo, boolean ignoreAutoincrementColumn, int bufferSize) {
    return new DeduplicateRowBuffer(tiTableInfo, ignoreAutoincrementColumn, bufferSize);
  }

  static class DefaultRowBuffer extends RowBuffer {

    public DefaultRowBuffer(int bufferSize) {
      super(bufferSize);
    }

    @Override
    public boolean add(Row row) {
      if (isFull()) {
        throw new IllegalStateException("Row buffer is full!");
      }
      rows.add(row);
      return true;
    }
  }
}
