/*
 * Copyright 2021 TiDB Project Authors.
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

package io.tidb.bigdata.cdc;

import java.util.Objects;

/*
 * TiCDC open protocol event key
 */
public final class Key {

  private final long ts;
  private final String schema;
  private final String table;
  private final long rowId;
  private final long partition;
  private final Type type;

  public Key(final String schema, final String table, final long rowId, final long partition,
      final int type, final long ts) {
    this.schema = schema;
    this.table = table;
    this.rowId = rowId;
    this.partition = partition;
    this.ts = ts;
    switch (type) {
      case 1:
        this.type = Type.ROW_CHANGED;
        break;
      case 2:
        this.type = Type.DDL;
        break;
      case 3:
        this.type = Type.RESOLVED;
        break;
      default:
        throw new RuntimeException("Invalid event type: " + type);
    }
  }

  public long getTimestamp() {
    if (ts > 0) {
      return ts >> 18;
    }
    return -1L;
  }

  public long getTs() {
    return ts;
  }

  public String getSchema() {
    return schema;
  }

  public String getTable() {
    return table;
  }

  public Type getType() {
    return type;
  }

  public long getPartition() {
    return partition;
  }

  public long getRowId() {
    return rowId;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Key)) {
      return false;
    }

    final Key other = (Key) o;
    return Objects.equals(ts, other.ts)
        && Objects.equals(schema, other.schema)
        && Objects.equals(table, other.table)
        && Objects.equals(rowId, other.rowId)
        && Objects.equals(partition, other.partition)
        && Objects.equals(type, other.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ts, schema, table, rowId, partition, type);
  }

  public enum Type {
    ROW_CHANGED,
    DDL,
    RESOLVED
  }
}
