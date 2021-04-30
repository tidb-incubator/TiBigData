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

import static io.tidb.bigdata.cdc.Misc.uncheckedRun;

import io.tidb.bigdata.cdc.Key;
import io.tidb.bigdata.cdc.Parser;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class CraftParser implements Parser<CraftParserState> {

  static final int KEY_SIZE_TABLE_INDEX = 0;
  static final int VALUE_SIZE_TABLE_INDEX = 1;
  static final int COLUMN_GROUP_SIZE_TABLE_START_INDEX = 2;
  private static final int CURRENT_VERSION = 1;
  private static final CraftParser INSTANCE = new CraftParser();

  private CraftParser() {
  }

  public static CraftParser getInstance() {
    return INSTANCE;
  }

  private static CraftParserState doParse(Codec codec, byte[] bits) throws IOException {
    if (codec.decodeUvarint() != CURRENT_VERSION) {
      throw new RuntimeException("Illegal version, should be " + CURRENT_VERSION);
    }
    int numOfPairs = (int) codec.decodeUvarint();
    int[][] sizeTables = parseSizeTables(codec);
    int keyBytes = sizeTables[KEY_SIZE_TABLE_INDEX][0];
    Key[] keys = parseKeys(codec, numOfPairs, keyBytes);
    return new CraftParserState(codec, keys, sizeTables);
  }

  private static Key[] parseKeys(Codec codec, int numOfKeys, int keyBytes) throws IOException {
    Codec keysCodec = codec.truncateHeading(keyBytes);
    long[] ts = keysCodec.decodeDeltaUvarintChunk(numOfKeys);
    long[] type = keysCodec.decodeDeltaUvarintChunk(numOfKeys);
    long[] rowId = keysCodec.decodeDeltaVarintChunk(numOfKeys);
    long[] partition = keysCodec.decodeDeltaVarintChunk(numOfKeys);
    String[] schema = keysCodec.decodeNullableStringChunk(numOfKeys);
    String[] table = keysCodec.decodeNullableStringChunk(numOfKeys);
    Key[] keys = new Key[numOfKeys];
    for (int idx = 0; idx < numOfKeys; idx++) {
      keys[idx] = new Key(schema[idx], table[idx], rowId[idx], partition[idx], (int) type[idx],
          ts[idx]);
    }
    return keys;
  }

  private static int[][] parseSizeTables(Codec codec) throws IOException {
    int size = codec.decodeUvarintReversedLength();
    Codec slice = codec.truncateTailing(size);
    ArrayList<int[]> tables = new ArrayList<>();
    while (slice.available() > 0) {
      int elements = slice.decodeUvarintLength();
      tables.add(
          Arrays.stream(slice.decodeDeltaUvarintChunk(elements)).mapToInt(l -> (int) l).toArray());
    }
    return tables.toArray(new int[0][]);
  }

  @Override
  public CraftParserState parse(byte[] bits) {
    return uncheckedRun(() -> doParse(new Codec(bits), bits));
  }
}