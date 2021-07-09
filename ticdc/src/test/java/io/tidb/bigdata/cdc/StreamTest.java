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

package io.tidb.bigdata.cdc;


import static io.tidb.bigdata.cdc.FileLoader.getFile;
import static io.tidb.bigdata.cdc.FileLoader.getFileContent;

import io.tidb.bigdata.cdc.json.JsonParser;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class StreamTest {

  private byte[][] loadFiles(final String prefix, final int number) throws IOException {
    final byte[][] files = new byte[number][];
    for (int idx = 0; idx < number; ++idx) {
      files[idx] = getFileContent(getFile(Codec.json(), prefix + idx, true));
    }
    return files;
  }

  private void testEvents(final JsonParser parser, final String prefix, final int number)
      throws IOException, InterruptedException {
    final byte[][] keys = loadFiles("key/" + prefix, number);
    final byte[][] values = loadFiles("value/" + prefix, number);
    final EventStream stream = new EventStream();
    int expectedCount = 0;
    for (int idx = 0; idx < number; ++idx) {
      expectedCount += stream
          .put(0, EventChunkDecoder.create(keys[idx], values[idx], parser).next());
    }

    Event event;
    long lastTs = -1;
    while ((event = stream.poll(0)) != null) {
      Assert.assertTrue(lastTs < event.getTs());
      lastTs = event.getTs();
      expectedCount--;
    }

    Assert.assertEquals(expectedCount, 0);
  }

  @Test
  public void test() throws IOException, InterruptedException {
    JsonParser parser = ParserFactory.json().createParser();
    testEvents(parser, "ddl_", 3);
    testEvents(parser, "row_", 3);
    testEvents(parser, "rts_", 3);
  }
}
