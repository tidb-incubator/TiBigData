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

import io.tidb.bigdata.tidb.codec.CodecDataInput;
import io.tidb.bigdata.tidb.columnar.TiBlockColumnVector;
import io.tidb.bigdata.tidb.types.DataType;
import io.tidb.bigdata.tidb.util.MemoryUtil;
import java.nio.ByteBuffer;

// TODO Support nullable data types.
// TODO Support nested, array and struct types.
public abstract class CHType {
  protected int length;
  protected boolean nullable = false;

  abstract String name();

  public boolean isNullable() {
    return nullable;
  }

  public void setNullable(boolean nullable) {
    this.nullable = nullable;
  }

  protected ByteBuffer decodeNullMap(CodecDataInput cdi, int size) {
    // read size * uint8 from cdi
    ByteBuffer buffer = allocate(size);
    MemoryUtil.readFully(buffer, cdi, size);
    buffer.clear();
    return buffer;
  }

  public abstract DataType toDataType();

  protected int bufferSize(int size) {
    return size * length;
  }

  public TiBlockColumnVector decode(CodecDataInput cdi, int size) {
    if (length == -1) {
      throw new IllegalStateException("var type should have its own decode method");
    }

    if (size == 0) {
      return new TiBlockColumnVector(this);
    }
    if (isNullable()) {
      ByteBuffer nullMap = decodeNullMap(cdi, size);
      ByteBuffer data = allocate(bufferSize(size));
      // read bytes from cdi to buffer(off-heap)
      MemoryUtil.readFully(data, cdi, bufferSize(size));
      data.clear();
      return new TiBlockColumnVector(this, nullMap, data, size, length);
    } else {
      ByteBuffer buffer = allocate(bufferSize(size));
      MemoryUtil.readFully(buffer, cdi, bufferSize(size));
      buffer.clear();
      return new TiBlockColumnVector(this, buffer, size, length);
    }
  }
}
