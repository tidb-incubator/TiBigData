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

package com.zhihu.tibigdata.prestodb.tidb;

import static java.lang.String.format;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.zhihu.tibigdata.prestodb.tidb.TypeHelper.BooleanRecordCursorReader;
import com.zhihu.tibigdata.prestodb.tidb.TypeHelper.DoubleRecordCursorReader;
import com.zhihu.tibigdata.prestodb.tidb.TypeHelper.LongRecordCursorReader;
import com.zhihu.tibigdata.prestodb.tidb.TypeHelper.RecordCursorReader;
import com.zhihu.tibigdata.prestodb.tidb.TypeHelper.SliceRecordCursorReader;
import com.zhihu.tibigdata.tidb.RecordCursorInternal;
import com.zhihu.tibigdata.tidb.Wrapper;
import io.airlift.slice.Slice;
import java.util.List;
import org.tikv.common.types.DataType;

public final class TiDBRecordCursor extends Wrapper<RecordCursorInternal> implements RecordCursor {

  private final BooleanRecordCursorReader[] booleanReaders;
  private final DoubleRecordCursorReader[] doubleReaders;
  private final LongRecordCursorReader[] longReaders;
  private final SliceRecordCursorReader[] sliceReaders;

  private TiDBColumnHandle[] columnHandles;

  public TiDBRecordCursor(List<TiDBColumnHandle> columnHandles, List<DataType> columnTypes,
      RecordCursorInternal internal) {
    super(internal);
    int numColumns = columnHandles.size();
    this.columnHandles = columnHandles.toArray(new TiDBColumnHandle[numColumns]);
    booleanReaders = new BooleanRecordCursorReader[numColumns];
    doubleReaders = new DoubleRecordCursorReader[numColumns];
    longReaders = new LongRecordCursorReader[numColumns];
    sliceReaders = new SliceRecordCursorReader[numColumns];

    for (int idx = 0; idx < numColumns; idx++) {
      TiDBColumnHandle column = this.columnHandles[idx];
      Class<?> javaType = column.getPrestoType().getJavaType();
      RecordCursorReader reader = column.getTypeHelper().getReader();

      if (javaType == boolean.class) {
        booleanReaders[idx] = (BooleanRecordCursorReader) reader;
      } else if (javaType == double.class) {
        doubleReaders[idx] = (DoubleRecordCursorReader) reader;
      } else if (javaType == long.class) {
        longReaders[idx] = (LongRecordCursorReader) reader;
      } else if (javaType == Slice.class) {
        sliceReaders[idx] = (SliceRecordCursorReader) reader;
      } else {
        throw new IllegalStateException(format("Unsupported java type %s", javaType));
      }
    }
  }

  @Override
  public long getCompletedBytes() {
    return 0;
  }

  @Override
  public long getReadTimeNanos() {
    return 0;
  }

  @Override
  public Type getType(int field) {
    return columnHandles[field].getPrestoType();
  }

  @Override
  public boolean advanceNextPosition() {
    return getInternal().advanceNextPosition();
  }

  @Override
  public boolean getBoolean(int field) {
    return booleanReaders[field].read(getInternal(), field);
  }

  @Override
  public long getLong(int field) {
    return longReaders[field].read(getInternal(), field);
  }

  @Override
  public double getDouble(int field) {
    return doubleReaders[field].read(getInternal(), field);
  }

  @Override
  public Slice getSlice(int field) {
    return sliceReaders[field].read(getInternal(), field);
  }

  @Override
  public Object getObject(int field) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isNull(int field) {
    return getInternal().isNull(field);
  }

  @Override
  public void close() {
  }
}
