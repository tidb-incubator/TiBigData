/*
 * Copyright 2021 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.tidb.bigdata.tidb.columnar.datatypes;

import static io.tidb.bigdata.tidb.util.MemoryUtil.allocate;

import com.google.common.base.Preconditions;
import io.tidb.bigdata.tidb.codec.Codec.IntegerCodec;
import io.tidb.bigdata.tidb.codec.CodecDataInput;
import io.tidb.bigdata.tidb.columnar.TiBlockColumnVector;
import io.tidb.bigdata.tidb.types.DataType;
import io.tidb.bigdata.tidb.types.StringType;
import io.tidb.bigdata.tidb.util.MemoryUtil;
import java.nio.ByteBuffer;

public class CHTypeString extends CHType {
  // Use to prevent frequently reallocate the chars buffer.
  // ClickHouse does not pass a total length at the beginning, so sad...
  private static final ThreadLocal<ByteBuffer> initBuffer =
      ThreadLocal.withInitial(() -> allocate(102400));

  public CHTypeString() {
    this.length = -1;
  }

  @Override
  public String name() {
    return "String";
  }

  @Override
  public DataType toDataType() {
    return StringType.TEXT;
  }

  @Override
  public TiBlockColumnVector decode(CodecDataInput cdi, int size) {
    if (size == 0) {
      return new TiBlockColumnVector(this);
    }

    ByteBuffer nullMap;
    if (isNullable()) {
      nullMap = decodeNullMap(cdi, size);
    } else {
      nullMap = null;
    }

    ByteBuffer offsets = allocate(size << 3);
    ByteBuffer initDataBuf = initBuffer.get();
    io.tidb.bigdata.tidb.columnar.datatypes.AutoGrowByteBuffer autoGrowDataBuf =
        new AutoGrowByteBuffer(initDataBuf);

    int offset = 0;
    for (int i = 0; i < size; i++) {
      int valueSize = (int) IntegerCodec.readUVarLong(cdi);

      offset += valueSize + 1;
      offsets.putLong(offset);

      autoGrowDataBuf.put(cdi, valueSize);
      autoGrowDataBuf.putByte((byte) 0); // terminating zero byte
    }

    Preconditions.checkState(offset == autoGrowDataBuf.dataSize());

    ByteBuffer data = autoGrowDataBuf.getByteBuffer();
    if (data == initDataBuf) {
      // Copy out.
      data = MemoryUtil.copyOf(data, offset);
    }

    return new TiBlockColumnVector(this, nullMap, offsets, data, size);
  }
}
