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

package io.tidb.bigdata.cdc.craft;

import io.tidb.bigdata.cdc.DdlValue;
import io.tidb.bigdata.cdc.Event;
import io.tidb.bigdata.cdc.Key;
import io.tidb.bigdata.cdc.Misc;
import io.tidb.bigdata.cdc.ResolvedValue;
import io.tidb.bigdata.cdc.RowChangedValue;
import io.tidb.bigdata.cdc.RowColumn;
import io.tidb.bigdata.cdc.RowDeletedValue;
import io.tidb.bigdata.cdc.RowInsertedValue;
import io.tidb.bigdata.cdc.RowUpdatedValue;
import io.tidb.bigdata.cdc.Value;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;

public class CraftParserState implements Iterator<Event> {

  private static final byte COLUMN_GROUP_TYPE_DELETE = 0x3;
  private static final byte COLUMN_GROUP_TYPE_OLD = 0x2;
  private static final byte COLUMN_GROUP_TYPE_NEW = 0x1;

  private static final short MYSQL_TYPE_DECIMAL = 0;
  private static final short MYSQL_TYPE_TINY = 1;
  private static final short MYSQL_TYPE_SHORT = 2;
  private static final short MYSQL_TYPE_LONG = 3;
  private static final short MYSQL_TYPE_FLOAT = 4;
  private static final short MYSQL_TYPE_DOUBLE = 5;
  private static final short MYSQL_TYPE_NULL = 6;
  private static final short MYSQL_TYPE_TIMESTAMP = 7;
  private static final short MYSQL_TYPE_LONGLONG = 8;
  private static final short MYSQL_TYPE_INT24 = 9;
  private static final short MYSQL_TYPE_DATE = 10;
  /*
   * private static final short MYSQL_TYPE_DURATION original name was private static final
   * short MYSQL_TYPE_Time, renamed to private static final short MYSQL_TYPE_Duration to
   * resolve the conflict with Go type Time.
   */
  private static final short MYSQL_TYPE_DURATION = 11;
  private static final short MYSQL_TYPE_DATETIME = 12;
  private static final short MYSQL_TYPE_YEAR = 13;
  private static final short MYSQL_TYPE_NEWDATE = 14;
  private static final short MYSQL_TYPE_VARCHAR = 15;
  private static final short MYSQL_TYPE_BIT = 16;

  private static final short MYSQL_TYPE_JSON = 0xf5;
  private static final short MYSQL_TYPE_NEWDECIMAL = 0xf6;
  private static final short MYSQL_TYPE_ENUM = 0xf7;
  private static final short MYSQL_TYPE_SET = 0xf8;
  private static final short MYSQL_TYPE_TINYBLOB = 0xf9;
  private static final short MYSQL_TYPE_MEDIUMBLOB = 0xfa;
  private static final short MYSQL_TYPE_LONGBLOB = 0xfb;
  private static final short MYSQL_TYPE_BLOB = 0xfc;
  private static final short MYSQL_TYPE_VARSTRING = 0xfd;
  private static final short MYSQL_TYPE_STRING = 0xfe;
  private static final short MYSQL_TYPE_GEOMETRY = 0xff;

  private static final long UNSIGNED_FLAG = 1 << 7;

  private final Codec codec;
  private final Key[] keys;
  private final int[][] sizeTables;
  private final int[] valueSizeTable;
  private final int[] columnGroupSizeTable;
  private final Event[] events;
  private int index;
  private int columnGroupIndex;

  CraftParserState(Codec codec, Key[] keys, int[][] sizeTables) {
    this(codec, keys, sizeTables, new Event[keys.length]);
  }

  private CraftParserState(Codec codec, Key[] keys, int[][] sizeTables, Event[] events) {
    this.codec = codec;
    this.keys = keys;
    this.index = 0;
    this.events = events;
    this.sizeTables = sizeTables;
    this.valueSizeTable = sizeTables[CraftParser.VALUE_SIZE_TABLE_INDEX];
    if (sizeTables.length > CraftParser.COLUMN_GROUP_SIZE_TABLE_START_INDEX) {
      this.columnGroupSizeTable = sizeTables[CraftParser.COLUMN_GROUP_SIZE_TABLE_START_INDEX];
    } else {
      this.columnGroupSizeTable = null;
    }
  }

  @Override
  public CraftParserState clone() {
    return new CraftParserState(codec.clone(), keys, sizeTables,
        Arrays.copyOf(events, events.length));
  }

  @Override
  public boolean hasNext() {
    return index < keys.length;
  }

  @Override
  public Event next() {
    Event event = events[index];
    if (event != null) {
      return event;
    }
    event = events[index] = new Event(keys[index], Misc.uncheckedRun(this::decodeValue));
    index++;
    return event;
  }

  private DdlValue decodeDdl(Codec codec) throws IOException {
    long type = codec.decodeUvarint();
    String query = codec.decodeString();
    return new DdlValue(query, (int) type);
  }

  private Object decodeTiDBType(long type, long flags, byte[] value) throws IOException {
    if (value == null) {
      return null;
    }
    switch ((int) type) {
      case MYSQL_TYPE_DATE:
        // FALLTHROUGH
      case MYSQL_TYPE_DATETIME:
        // FALLTHROUGH
      case MYSQL_TYPE_NEWDATE:
        // FALLTHROUGH
      case MYSQL_TYPE_TIMESTAMP:
        // FALLTHROUGH
      case MYSQL_TYPE_DURATION:
        // FALLTHROUGH
      case MYSQL_TYPE_JSON:
        // FALLTHROUGH
      case MYSQL_TYPE_NEWDECIMAL:
        // value type for these mysql types are string
        return new String(value, StandardCharsets.UTF_8);
      case MYSQL_TYPE_ENUM:
        // FALLTHROUGH
      case MYSQL_TYPE_SET:
        // FALLTHROUGH
      case MYSQL_TYPE_BIT:
        // value type for thest mysql types are uint64
        return new Codec(value).decodeUvarint();
      case MYSQL_TYPE_STRING:
      case MYSQL_TYPE_VARSTRING:
      case MYSQL_TYPE_VARCHAR:
      case MYSQL_TYPE_TINYBLOB:
      case MYSQL_TYPE_MEDIUMBLOB:
      case MYSQL_TYPE_LONGBLOB:
      case MYSQL_TYPE_BLOB:
        // value type for these mysql types are []byte
        return value;
      case MYSQL_TYPE_FLOAT:
      case MYSQL_TYPE_DOUBLE:
        // value type for these mysql types are float64
        return new Codec(value).decodeFloat64();
      case MYSQL_TYPE_TINY:
      case MYSQL_TYPE_SHORT:
      case MYSQL_TYPE_LONG:
      case MYSQL_TYPE_LONGLONG:
      case MYSQL_TYPE_INT24:
      case MYSQL_TYPE_YEAR:
        // value type for these mysql types are int64 or uint64 depends on flags
        Codec codec = new Codec(value);
        if ((flags & UNSIGNED_FLAG) == UNSIGNED_FLAG) {
          return codec.decodeUvarint();
        } else {
          return codec.decodeVarint();
        }
      case MYSQL_TYPE_NULL:
        // FALLTHROUGH
      case MYSQL_TYPE_GEOMETRY:
        // FALLTHROUGH
      default:
        return null;
    }
  }

  private RowColumn[] decodeColumnGroup(Codec codec) throws IOException {
    int numOfColumns = (int) codec.decodeUvarint();
    String[] names = codec.decodeStringChunk(numOfColumns);
    long[] types = codec.decodeUvarintChunk(numOfColumns);
    long[] flags = codec.decodeUvarintChunk(numOfColumns);
    byte[][] values = codec.decodeNullableBytesChunk(numOfColumns);

    RowColumn[] columns = new RowColumn[numOfColumns];
    for (int idx = 0; idx < numOfColumns; idx++) {
      columns[idx] = new RowColumn(names[idx],
          decodeTiDBType(types[idx], flags[idx], values[idx]),
          false, (int) types[idx], flags[idx]);
    }

    return columns;
  }

  private RowChangedValue decodeRowChanged(Codec codec) throws IOException {
    RowColumn[] oldColumns = null;
    RowColumn[] newColumns = null;
    while (codec.available() > 0) {
      final int size = columnGroupSizeTable[columnGroupIndex++];
      final Codec columnGroupCodec = codec.truncateHeading(size);
      byte type = (byte) columnGroupCodec.decodeUint8();
      RowColumn[] columns = decodeColumnGroup(columnGroupCodec);
      switch (type) {
        case COLUMN_GROUP_TYPE_DELETE:
          return new RowDeletedValue(columns);
        case COLUMN_GROUP_TYPE_OLD:
          oldColumns = columns;
          break;
        case COLUMN_GROUP_TYPE_NEW:
          newColumns = columns;
          break;
        default:
          throw new IllegalStateException("Unknown column group type: " + type);
      }
    }
    if (oldColumns != null) {
      return new RowUpdatedValue(oldColumns, newColumns);
    } else {
      return new RowInsertedValue(newColumns);
    }
  }

  private Value decodeValue() throws IOException {
    int size = valueSizeTable[index];
    Key.Type type = keys[index].getType();
    switch (keys[index].getType()) {
      case DDL:
        return decodeDdl(codec.truncateHeading(size));
      case RESOLVED:
        return ResolvedValue.getInstance();
      case ROW_CHANGED:
        return decodeRowChanged(codec.truncateHeading(size));
      default:
        throw new IllegalStateException("Unknown value type: " + type);
    }
  }
}