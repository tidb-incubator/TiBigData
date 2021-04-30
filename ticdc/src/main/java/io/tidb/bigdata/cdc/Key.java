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

import io.tidb.bigdata.cdc.json.jackson.JacksonFactory;
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
    this.type = Type.of(type);
  }

  public static long fromTimestamp(long ms) {
    if (ms > 0) {
      return ms << 18;
    }
    return -1L;
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

  public String toJson() {
    return toJson(Event.defaultJacksonFactory);
  }

  public String toJson(JacksonFactory factory) {
    return factory.toJson(factory.createObject()
        .put("ts", getTs())
        .put("scm", getSchema())
        .put("tbl", getTable())
        .put("t", getType().code())
    );
  }

  public enum Type {
    ROW_CHANGED(1),
    DDL(2),
    RESOLVED(3);

    private int code;

    Type(int code) {
      this.code = code;
    }

    public static Type of(final int code) {
      switch (code) {
        case 1:
          return ROW_CHANGED;
        case 2:
          return DDL;
        case 3:
          return RESOLVED;
        default:
          throw new IllegalArgumentException("Invalid event type code: " + code);
      }
    }

    public int code() {
      return this.code;
    }
  }
}
