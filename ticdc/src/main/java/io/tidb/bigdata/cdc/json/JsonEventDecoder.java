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

package io.tidb.bigdata.cdc.json;

import io.tidb.bigdata.cdc.Event;
import io.tidb.bigdata.cdc.EventDecoder;
import java.util.Arrays;
import java.util.Iterator;

/**
 * TiCDC open protocol event decoder, parse key value pairs into event instances.
 */
public class JsonEventDecoder implements EventDecoder {

  private final Event[] events;
  private final Iterator<Event> iterator;

  public JsonEventDecoder(final byte[] key, final byte[] value, final JsonParser parser) {
    this.events = new JsonEventChunkDecoder(key, value, parser).next();
    this.iterator = iterator();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Event next() {
    return iterator.next();
  }

  @Override
  @SuppressWarnings("NullableProblems")
  public Iterator<Event> iterator() {
    return Arrays.stream(events).iterator();
  }
}
