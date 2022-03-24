/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import static java.lang.String.format;

import io.tidb.bigdata.flink.connector.source.TiDBMetadata;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.tikv.common.meta.TiTimestamp;

public final class TiDBCanalJsonDeserializationSchema implements DeserializationSchema<RowData> {

  private static final long serialVersionUID = 1L;

  private static final String FIELD_OLD = "old";
  private static final String OP_INSERT = "INSERT";
  private static final String OP_UPDATE = "UPDATE";
  private static final String OP_DELETE = "DELETE";
  private static final String OP_CREATE = "CREATE";


  private final Set<String> schemas;
  private final Set<String> tables;
  private final CDCMetadata[] metadata;
  private final long startTs;

  /**
   * The deserializer to deserialize Canal JSON data.
   */
  private final JsonRowDataDeserializationSchema jsonDeserializer;

  /**
   * {@link TypeInformation} of the produced {@link RowData} (physical + meta data).
   */
  private final TypeInformation<RowData> producedTypeInfo;

  /**
   * Flag indicating whether to ignore invalid fields/rows (default: throw an exception).
   */
  private final boolean ignoreParseErrors;

  /**
   * Names of fields.
   */
  private final List<String> fieldNames;

  /**
   * Number of fields.
   */
  private final int fieldCount;

  public TiDBCanalJsonDeserializationSchema(
      DataType physicalDataType, Set<String> schemas,
      Set<String> tables, CDCMetadata[] metadata,
      long startTs, TypeInformation<RowData> producedTypeInfo,
      boolean ignoreParseErrors,
      TimestampFormat timestampFormat) {
    this.schemas = schemas;
    this.tables = tables;
    this.metadata = metadata;
    this.startTs = startTs;
    final RowType jsonRowType = createJsonRowType(physicalDataType);
    this.jsonDeserializer =
        new JsonRowDataDeserializationSchema(
            jsonRowType,
            // the result type is never used, so it's fine to pass in the produced type
            // info
            producedTypeInfo,
            false, // ignoreParseErrors already contains the functionality of
            // failOnMissingField
            ignoreParseErrors,
            timestampFormat);
    this.producedTypeInfo = producedTypeInfo;
    this.ignoreParseErrors = ignoreParseErrors;
    final RowType physicalRowType = ((RowType) physicalDataType.getLogicalType());
    this.fieldNames = physicalRowType.getFieldNames();
    this.fieldCount = physicalRowType.getFieldCount();
  }

  // ------------------------------------------------------------------------------------------

  @Override
  public RowData deserialize(byte[] message) throws IOException {
    throw new RuntimeException(
        "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
  }

  @Override
  public void deserialize(@Nullable byte[] message, Collector<RowData> out) throws IOException {
    if (message == null || message.length == 0) {
      return;
    }
    try {
      final JsonNode root = jsonDeserializer.deserializeToJsonNode(message);
      Optional<JsonNode> commitTs = Optional.ofNullable(root.get(_TIDB))
          .map(tidbExtension -> tidbExtension.get(COMMIT_TS));
      if (!commitTs.isPresent()) {
        return;
      }
      long tso = commitTs.get().asLong();
      if (tso < startTs) {
        return;
      }
      TiTimestamp timestamp = new TiTimestamp(tso >> 18, tso & 0x3FFFF);
      if (schemas != null && schemas.size() > 0) {
        if (!schemas.contains(root.get(DATABASE).asText())) {
          return;
        }
      }
      if (tables != null && tables.size() > 0) {
        if (!tables.contains(root.get(TABLE).asText())) {
          return;
        }
      }

      final GenericRowData row = (GenericRowData) jsonDeserializer.convertToRowData(root);
      String type = row.getString(2).toString(); // "type" field
      if (OP_INSERT.equals(type)) {
        // "data" field is an array of row, contains inserted rows
        ArrayData data = row.getArray(0);
        for (int i = 0; i < data.size(); i++) {
          GenericRowData insert = (GenericRowData) data.getRow(i, fieldCount);
          insert.setRowKind(RowKind.INSERT);
          emitRow(insert, out, timestamp);
        }
      } else if (OP_UPDATE.equals(type)) {
        // "data" field is an array of row, contains new rows
        ArrayData data = row.getArray(0);
        // "old" field is an array of row, contains old values
        ArrayData old = row.getArray(1);
        for (int i = 0; i < data.size(); i++) {
          // the underlying JSON deserialization schema always produce GenericRowData.
          GenericRowData after = (GenericRowData) data.getRow(i, fieldCount);
          GenericRowData before = (GenericRowData) old.getRow(i, fieldCount);
          final JsonNode oldField = root.get(FIELD_OLD);
          for (int f = 0; f < fieldCount; f++) {
            if (before.isNullAt(f) && oldField.findValue(fieldNames.get(f)) == null) {
              // fields in "old" (before) means the fields are changed
              // fields not in "old" (before) means the fields are not changed
              // so we just copy the not changed fields into before
              before.setField(f, after.getField(f));
            }
          }
          before.setRowKind(RowKind.UPDATE_BEFORE);
          after.setRowKind(RowKind.UPDATE_AFTER);
          emitRow(before, out, timestamp);
          emitRow(after, out, timestamp);
        }
      } else if (OP_DELETE.equals(type)) {
        // "data" field is an array of row, contains deleted rows
        ArrayData data = row.getArray(0);
        for (int i = 0; i < data.size(); i++) {
          GenericRowData insert = (GenericRowData) data.getRow(i, fieldCount);
          insert.setRowKind(RowKind.DELETE);
          emitRow(insert, out, timestamp);
        }
      } else if (OP_CREATE.equals(type)) {
        // "data" field is null and "type" is "CREATE" which means
        // this is a DDL change event, and we should skip it.
        return;
      } else {
        if (!ignoreParseErrors) {
          throw new IOException(
              format(
                  "Unknown \"type\" value \"%s\". The Canal JSON message is '%s'",
                  type, new String(message)));
        }
      }
    } catch (Throwable t) {
      // a big try catch to protect the processing.
      if (!ignoreParseErrors) {
        throw new IOException(
            format("Corrupt Canal JSON message '%s'.", new String(message)), t);
      }
    }
  }

  private void emitRow(GenericRowData physicalRow, Collector<RowData> out, TiTimestamp timestamp) {
    if (metadata.length == 0) {
      out.collect(physicalRow);
      return;
    }
    GenericRowData rowData = new GenericRowData(physicalRow.getArity() + metadata.length);
    int i;
    for (i = 0; i <= physicalRow.getArity() - 1; i++) {
      rowData.setField(i, physicalRow.getField(i));
      rowData.setRowKind(physicalRow.getRowKind());
    }
    while (i <= physicalRow.getArity() + metadata.length - 1) {
      CDCMetadata cdcMetadata = metadata[i - physicalRow.getArity()];
      TiDBMetadata tiDBMetadata = cdcMetadata.toTiDBMetadata()
          .orElseThrow(() -> new IllegalArgumentException("Unsupported metadata: " + cdcMetadata));
      rowData.setField(i, tiDBMetadata.extract(timestamp));
      i++;
    }
    out.collect(rowData);
  }

  @Override
  public boolean isEndOfStream(RowData nextElement) {
    return false;
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return producedTypeInfo;
  }

  // --------------------------------------------------------------------------------------------

  private static final String DATA = "data";
  private static final String OLD = "old";
  private static final String TYPE = "type";
  private static final String DATABASE = "database";
  private static final String TABLE = "table";
  private static final String _TIDB = "_tidb";
  private static final String COMMIT_TS = "commitTs";

  private static RowType createJsonRowType(DataType physicalDataType) {
    // Canal JSON contains other information, e.g. "ts", "sql", but we don't need them
    DataType root =
        DataTypes.ROW(
            DataTypes.FIELD(DATA, DataTypes.ARRAY(physicalDataType)),
            DataTypes.FIELD(OLD, DataTypes.ARRAY(physicalDataType)),
            DataTypes.FIELD(TYPE, DataTypes.STRING()),
            DataTypes.FIELD(TABLE, DataTypes.STRING()),
            DataTypes.FIELD(DATABASE, DataTypes.STRING()));
    return (RowType) root.getLogicalType();
  }


}
