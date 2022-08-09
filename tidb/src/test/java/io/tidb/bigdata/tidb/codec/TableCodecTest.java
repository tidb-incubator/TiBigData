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

package io.tidb.bigdata.tidb.codec;

import static org.junit.Assert.assertArrayEquals;

import io.tidb.bigdata.tidb.handle.CommonHandle;
import io.tidb.bigdata.tidb.handle.Handle;
import io.tidb.bigdata.tidb.handle.IntHandle;
import io.tidb.bigdata.tidb.types.DataType;
import io.tidb.bigdata.tidb.types.StringType;
import org.junit.Test;

public class TableCodecTest {
  @Test
  public void testIndexValueCodec() {
    Handle commonHandle =
        CommonHandle.newCommonHandle(new DataType[] {StringType.VARCHAR}, new Object[] {"1"});
    // test common handle version0
    byte[] version0Value = TableCodec.genIndexValue(commonHandle, 0, true);
    Handle decodeCommonHandle0 = TableCodec.decodeHandle(version0Value, true);
    assertArrayEquals(commonHandle.encoded(), decodeCommonHandle0.encoded());

    // test common handle version1
    byte[] version1Value = TableCodec.genIndexValue(commonHandle, 1, true);
    Handle decodeCommonHandle1 = TableCodec.decodeHandle(version1Value, true);
    assertArrayEquals(commonHandle.encoded(), decodeCommonHandle1.encoded());

    // test int handle
    Handle intHandle = new IntHandle(1);
    byte[] intHandleValue = TableCodec.genIndexValue(intHandle, 0, true);
    Handle decodeIntHandle = TableCodec.decodeHandle(intHandleValue, false);
    assertArrayEquals(intHandle.encoded(), decodeIntHandle.encoded());
  }
}
