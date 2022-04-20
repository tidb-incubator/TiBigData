/*
 * Copyright 2021 TiKV Project Authors.
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

import io.tidb.bigdata.tidb.codec.CodecDataInput;
import io.tidb.bigdata.tidb.util.MemoryUtil;
import java.nio.ByteBuffer;

public class AutoGrowByteBuffer {
  private ByteBuffer buf;

  public AutoGrowByteBuffer(ByteBuffer initBuf) {
    initBuf.clear();
    this.buf = initBuf;
  }

  public int dataSize() {
    return buf.position();
  }

  public ByteBuffer getByteBuffer() {
    return buf;
  }

  private void beforeIncrease(int inc) {
    int minCap = buf.position() + inc;
    int newCap = buf.capacity();
    if (minCap > newCap) {
      do {
        newCap = newCap << 1;
      } while (minCap > newCap);
      buf = MemoryUtil.copyOf(buf, newCap);
    }
  }

  public void put(CodecDataInput cdi, int len) {
    beforeIncrease(len);

    buf.limit(buf.position() + len);
    MemoryUtil.readFully(buf, cdi, len);
  }

  public void putByte(byte v) {
    beforeIncrease(1);

    buf.limit(buf.position() + 1);
    buf.put(v);
  }
}
