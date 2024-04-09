/*
 * Copyright 2024 TiDB Project Authors.
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

package io.tidb.bigdata.tidb.meta;

import static org.junit.Assert.assertEquals;

import io.tidb.bigdata.tidb.types.TimestampType;
import org.junit.Test;

public class TiColumnInfoTest {

  @Test
  public void testToProto() {
    TiColumnInfo columnInfo =
        new TiColumnInfo(
            1L,
            "name",
            0,
            TimestampType.TIMESTAMP,
            SchemaState.StatePublic,
            "0000-00-00 00:00:00",
            "2024-04-01 00:00:00",
            "0000-00-00 00:00:00",
            "timestamp",
            1,
            "",
            false);
    assertEquals(
        "\000", columnInfo.getOriginDefaultValueAsByteString().toStringUtf8());
  }
}
