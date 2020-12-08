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

package com.zhihu.tibigdata.prestosql.tidb;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.zhihu.tibigdata.tidb.RecordCursorInternal;
import io.airlift.slice.Slice;
import io.prestosql.spi.type.Type;
import java.util.Objects;
import java.util.function.Function;
import org.tikv.common.types.DataType;

public final class TypeHelper {

  private final Type prestoType;
  private final DataType tidbType;
  private final RecordCursorReader cursorReader;
  private final Function toTiDBConverter;

  private TypeHelper(DataType tidbType, Type prestoType, RecordCursorReader cursorReader,
      Function toTiDBConverter) {
    this.prestoType = requireNonNull(prestoType, "prestoType is null");
    this.cursorReader = requireNonNull(cursorReader, "cursorReader is null");
    this.tidbType = requireNonNull(tidbType, "tidbType is null");
    this.toTiDBConverter = requireNonNull(toTiDBConverter, "toTiDBConverter is null");
  }

  static TypeHelper booleanHelper(DataType tidbType, Type prestoType,
      BooleanRecordCursorReader cursorReader, Function<Boolean, Object> toTiDBConverter) {
    return new TypeHelper(tidbType, prestoType, cursorReader, toTiDBConverter);
  }

  static TypeHelper booleanHelper(DataType tidbType, Type prestoType,
      BooleanRecordCursorReader cursorReader) {
    return booleanHelper(tidbType, prestoType, cursorReader, f -> f);
  }

  static TypeHelper longHelper(DataType tidbType, Type prestoType,
      LongRecordCursorReader cursorReader, Function<Long, Object> toTiDBConverter) {
    return new TypeHelper(tidbType, prestoType, cursorReader, toTiDBConverter);
  }

  static TypeHelper longHelper(DataType tidbType, Type prestoType,
      LongRecordCursorReader cursorReader) {
    return longHelper(tidbType, prestoType, cursorReader, f -> f);
  }

  static TypeHelper doubleHelper(DataType tidbType, Type prestoType,
      DoubleRecordCursorReader cursorReader, Function<Double, Object> toTiDBConverter) {
    return new TypeHelper(tidbType, prestoType, cursorReader, toTiDBConverter);
  }

  static TypeHelper doubleHelper(DataType tidbType, Type prestoType,
      DoubleRecordCursorReader cursorReader) {
    return doubleHelper(tidbType, prestoType, cursorReader, f -> f);
  }

  static TypeHelper sliceHelper(DataType tidbType, Type prestoType,
      SliceRecordCursorReader cursorReader, Function<Slice, Object> toTiDBConverter) {
    return new TypeHelper(tidbType, prestoType, cursorReader, toTiDBConverter);
  }

  static TypeHelper sliceHelper(DataType tidbType, Type prestoType,
      SliceRecordCursorReader cursorReader) {
    return sliceHelper(tidbType, prestoType, cursorReader, Slice::toStringUtf8);
  }

  public Type getPrestoType() {
    return prestoType;
  }

  public DataType getTiDBType() {
    return tidbType;
  }

  public RecordCursorReader getReader() {
    return cursorReader;
  }

  public Function getConverter() {
    return toTiDBConverter;
  }

  public Object toTiDB(Object from) {
    return toTiDBConverter.apply(from);
  }

  @Override
  public int hashCode() {
    return Objects.hash(prestoType, tidbType, cursorReader, toTiDBConverter);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }

    TypeHelper other = (TypeHelper) obj;
    return Objects.equals(this.prestoType, other.prestoType)
        && Objects.equals(this.tidbType, other.tidbType)
        && Objects.equals(this.cursorReader, other.cursorReader)
        && Objects.equals(this.toTiDBConverter, other.toTiDBConverter);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("tidbType", tidbType)
        .add("prestoType", prestoType)
        .add("cursorReader", cursorReader)
        .add("toTiDBConverter", toTiDBConverter)
        .toString();
  }

  static interface RecordCursorReader {

  }

  static interface BooleanRecordCursorReader
      extends RecordCursorReader {

    boolean read(RecordCursorInternal cursor, int index);
  }

  static interface DoubleRecordCursorReader
      extends RecordCursorReader {

    double read(RecordCursorInternal cursor, int index);
  }

  static interface LongRecordCursorReader
      extends RecordCursorReader {

    long read(RecordCursorInternal cursor, int index);
  }

  static interface SliceRecordCursorReader
      extends RecordCursorReader {

    Slice read(RecordCursorInternal cursor, int index);
  }
}
