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

package io.tidb.bigdata.tidb.handle;

import java.util.Arrays;
import  io.tidb.bigdata.tidb.codec.Codec.IntegerCodec;
import  io.tidb.bigdata.tidb.codec.CodecDataOutput;
import org.tikv.common.exception.CodecException;
import io.tidb.bigdata.tidb.types.DataType;
import io.tidb.bigdata.tidb.types.IntegerType;

public class IntHandle implements Handle {

  private final long handle;
  private final int infFlag;

  public IntHandle(long handle) {
    this.handle = handle;
    this.infFlag = 0;
  }

  private IntHandle(long handle, int infFlag) {
    this.handle = handle;
    this.infFlag = infFlag;
  }

  @Override
  public boolean isInt() {
    return true;
  }

  @Override
  public long intValue() {
    return handle;
  }

  @Override
  public Handle next() {
    if (handle != Long.MAX_VALUE) {
      return new IntHandle(handle + 1);
    }
    return new IntHandle(handle, 1);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof IntHandle) {
      return ((IntHandle) other).intValue() == handle;
    }
    return false;
  }

  @Override
  public int compare(Handle other) {
    if (!other.isInt()) {
      throw new RuntimeException("IntHandle compares to CommonHandle");
    }
    if (infFlag != ((IntHandle) other).infFlag) {
      return infFlag - ((IntHandle) other).infFlag;
    }
    long val = intValue();
    long otherVal = other.intValue();
    if (val > otherVal) {
      return 1;
    } else if (val < otherVal) {
      return -1;
    }
    return 0;
  }

  @Override
  public byte[] encoded() {
    CodecDataOutput cdo = new CodecDataOutput();
    IntegerCodec.writeLong(cdo, handle);
    byte[] encoded = cdo.toBytes();
    if (infFlag == 1) {
      return Arrays.copyOf(encoded, encoded.length + 1);
    }
    return encoded;
  }

  @Override
  public byte[] encodedAsKey() {
    CodecDataOutput cdo = new CodecDataOutput();
    IntegerType.BIGINT.encode(cdo, DataType.EncodeType.KEY, handle);
    byte[] encoded = cdo.toBytes();
    if (infFlag == 1) {
      return Arrays.copyOf(encoded, encoded.length + 1);
    }
    return encoded;
  }

  @Override
  public int len() {
    if (infFlag == 1) {
      return 9;
    }
    return 8;
  }

  @Override
  public int numCols() {
    throw new CodecException("not supported in IntHandle");
  }

  @Override
  public byte[] encodedCol(int idx) {
    throw new CodecException("not supported in IntHandle");
  }

  @Override
  public Object[] data() {
    return new Object[]{handle};
  }

  @Override
  public String toString() {
    if (infFlag == -1) {
      return "-inf";
    } else if (infFlag == 1) {
      return "+inf";
    }
    return String.valueOf(handle);
  }
}