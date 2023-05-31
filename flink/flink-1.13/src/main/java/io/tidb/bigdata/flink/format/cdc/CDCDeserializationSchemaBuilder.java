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

import com.google.common.base.Preconditions;
import io.tidb.bigdata.cdc.Key;
import io.tidb.bigdata.flink.format.canal.CanalDeserializationSchema;
import io.tidb.bigdata.flink.format.canal.CanalJsonDeserializationSchema;
import io.tidb.bigdata.flink.format.canal.CanalProtobufDeserializationSchema;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class CDCDeserializationSchemaBuilder {

  private final DataType physicalDataType;
  private final CDCMetadata[] metadata;
  private final TypeInformation<RowData> typeInformation;
  private Set<Key.Type> eventTypes;
  private Set<String> schemas;
  private Set<String> tables;
  private long startTs;
  private boolean ignoreParseErrors;

  public CDCDeserializationSchemaBuilder(DataType physicalDataType, CDCMetadata[] metadata) {
    this.physicalDataType = Preconditions.checkNotNull(physicalDataType);
    this.metadata = Preconditions.checkNotNull(metadata);
    this.typeInformation = CDCSchemaAdapter.getProducedType(physicalDataType, metadata);
  }

  public CDCDeserializationSchemaBuilder types(final Set<Key.Type> eventTypes) {
    this.eventTypes = eventTypes;
    return this;
  }

  public CDCDeserializationSchemaBuilder schemas(final Set<String> schemas) {
    this.schemas = schemas;
    return this;
  }

  public CDCDeserializationSchemaBuilder tables(final Set<String> tables) {
    this.tables = tables;
    return this;
  }

  public CDCDeserializationSchemaBuilder startTs(final long ts) {
    this.startTs = ts;
    return this;
  }

  public CDCDeserializationSchemaBuilder ignoreParseErrors(boolean ignore) {
    this.ignoreParseErrors = ignore;
    return this;
  }

  public CraftDeserializationSchema craft() {
    return new CraftDeserializationSchema(
        new CDCSchemaAdapter(physicalDataType, metadata),
        eventTypes,
        schemas,
        tables,
        startTs,
        ignoreParseErrors);
  }

  public JsonDeserializationSchema json() {
    return new JsonDeserializationSchema(
        new CDCSchemaAdapter(physicalDataType, metadata),
        eventTypes,
        schemas,
        tables,
        startTs,
        ignoreParseErrors);
  }

  // We use a new decoder, since canal does not follow TiCDC open protocol.
  public CanalDeserializationSchema canalJson() {
    return new CanalJsonDeserializationSchema(
        physicalDataType, schemas, tables, metadata, startTs, typeInformation, ignoreParseErrors);
  }

  public CanalDeserializationSchema canalProtobuf() {
    return new CanalProtobufDeserializationSchema(
        physicalDataType, schemas, tables, metadata, startTs, typeInformation, ignoreParseErrors);
  }
}
