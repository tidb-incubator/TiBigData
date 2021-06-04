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

package io.tidb.bigdata.cdc.json.jackson;

import io.tidb.bigdata.cdc.ParserFactory;
import io.tidb.bigdata.cdc.json.JsonNode;
import io.tidb.bigdata.cdc.json.JsonParser;

public class JacksonParserFactory implements ParserFactory<JsonParser, JsonNode> {

  private final Object mapper;
  private final JacksonContext context;

  private JacksonParserFactory(final JacksonContext context) {
    this.context = context;
    this.mapper = context.newMapper();
  }

  public static JacksonParserFactory create() {
    return new JacksonParserFactory(JacksonContext.getDefaultContext());
  }

  public static JacksonParserFactory create(String shadePrefix) {
    return new JacksonParserFactory(new JacksonContext(shadePrefix));
  }

  public JsonParser createParser() {
    return new JacksonParser(context, context.newReader(mapper));
  }
}
