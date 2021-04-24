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

package io.tidb.bigdata.flink.format.cdc;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

public class FormatOptions {

  public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS =
      ConfigOptions.key("ignore-parse-errors")
          .booleanType()
          .defaultValue(false)
          .withDescription(
              "Optional flag to skip change events with parse errors instead of failing;\n"
                  + "fields are set to null in case of errors, false by default.");

  public static final ConfigOption<String> SCHEMA_INCLUDE =
      ConfigOptions.key("schema.include")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "Only read changelogs belong to the specific schema");

  public static final ConfigOption<String> TABLE_INCLUDE =
      ConfigOptions.key("table.include")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "Only read changelogs belong to the specific table");

  /**
   * Validator for craft decoding format.
   */
  public static void validateDecodingFormatOptions(ReadableConfig tableOptions) {
  }
}
