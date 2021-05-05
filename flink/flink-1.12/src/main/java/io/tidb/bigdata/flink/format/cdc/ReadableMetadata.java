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

import io.tidb.bigdata.cdc.Event;
import io.tidb.bigdata.cdc.json.jackson.JacksonFactory;
import java.util.function.Function;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

public enum ReadableMetadata {
  SCHEMA("schema", DataTypes.STRING().nullable(), ReadableMetadata::schema),
  TABLE("table", DataTypes.STRING().nullable(), ReadableMetadata::table),
  COMMIT_VERSION("commit_version", DataTypes.BIGINT().notNull(), Event::getTs),
  COMMIT_TIMESTAMP("commit_timestamp",
      DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
      ReadableMetadata::commitMs),
  TYPE("type", DataTypes.STRING().notNull(), ReadableMetadata::typeName),
  TYPE_CODE("type_code", DataTypes.INT().notNull(), ReadableMetadata::typeCode),
  KEY("key", DataTypes.STRING().nullable(), ReadableMetadata::key),
  VALUE("value", DataTypes.STRING().nullable(), ReadableMetadata::value);

  private static final JacksonFactory flinkShadedJackson =
      JacksonFactory.create("org.apache.flink.shaded.jackson2");
  final String key;
  final DataType type;
  final Function<Event, Object> extractor;

  ReadableMetadata(final String key, final DataType type, Function<Event, Object> extractor) {
    this.key = key;
    this.type = type;
    this.extractor = extractor;
  }

  private static Integer typeCode(final Event event) {
    return event.getType().code();
  }

  private static StringData typeName(final Event event) {
    return StringData.fromString(event.getType().name());
  }

  private static TimestampData commitMs(final Event event) {
    return TimestampData.fromEpochMillis(event.getTimestamp());
  }

  private static StringData schema(final Event event) {
    return StringData.fromString(event.getSchema());
  }

  private static StringData table(final Event event) {
    return StringData.fromString(event.getTable());
  }

  private static StringData key(final Event event) {
    return StringData.fromString(event.getKey().toJson(flinkShadedJackson));
  }

  private static StringData value(final Event event) {
    return StringData.fromString(event.getValue().toJson(flinkShadedJackson));
  }
}
