/*
 * Copyright 2023 TiDB Project Authors.
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

package io.tidb.bigdata.flink.format.canal;

import io.tidb.bigdata.flink.connector.source.TiDBMetadata;
import io.tidb.bigdata.flink.format.cdc.CDCMetadata;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.meta.TiTimestamp;

public abstract class CanalDeserializationSchema implements DeserializationSchema<RowData> {

  private static final Logger LOG = LoggerFactory.getLogger(CanalDeserializationSchema.class);

  private static final String INSERT = "INSERT";
  private static final String UPDATE = "UPDATE";
  private static final String DELETE = "DELETE";
  private static final String CREATE = "CREATE";
  private static final String QUERY = "QUERY";
  private static final String TIDB_WATERMARK = "TIDB_WATERMARK";

  public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
  public static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
  public static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  protected final DataType physicalDataType;
  protected final Set<String> schemas;
  protected final Set<String> tables;
  protected final CDCMetadata[] metadata;
  protected final long startTs;
  protected final TypeInformation<RowData> producedTypeInfo;
  protected final boolean ignoreParseErrors;

  protected final List<String> fieldNames;
  protected final List<DataType> dataTypes;

  protected CanalDeserializationSchema(
      DataType physicalDataType,
      Set<String> schemas,
      Set<String> tables,
      CDCMetadata[] metadata,
      long startTs,
      TypeInformation<RowData> producedTypeInfo,
      boolean ignoreParseErrors) {
    this.physicalDataType = physicalDataType;
    this.schemas = schemas;
    this.tables = tables;
    this.metadata = metadata;
    this.startTs = startTs;
    Preconditions.checkArgument(
        createTiTimestampFromVersion(startTs).getLogical() == 0,
        "The logical ts must be 0 for canal format");
    this.producedTypeInfo = producedTypeInfo;
    this.ignoreParseErrors = ignoreParseErrors;
    RowType physicalRowType = ((RowType) physicalDataType.getLogicalType());
    this.fieldNames = new ArrayList<>(physicalRowType.getFieldNames());
    this.dataTypes = new ArrayList<>(physicalDataType.getChildren());
  }

  @Override
  public final RowData deserialize(byte[] message) throws IOException {
    throw new UnsupportedOperationException(
        "Please invoke deserialize(byte[] message, Collector<RowData> out)");
  }

  @Override
  public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
    List<FlatMessage> flatMessages;
    try {
      flatMessages = decodeToFlatMessage(message);
    } catch (Exception e) {
      if (ignoreParseErrors) {
        LOG.warn("Can not decode to flatMessage", e);
      }
      throw new RuntimeException(e);
    }
    for (FlatMessage flatMessage : flatMessages) {
      deserialize(flatMessage, out);
    }
  }

  @Override
  public boolean isEndOfStream(RowData nextElement) {
    return false;
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return producedTypeInfo;
  }

  public abstract List<FlatMessage> decodeToFlatMessage(byte[] data) throws Exception;

  public void deserialize(FlatMessage flatMessage, Collector<RowData> out) {
    if (!schemas.contains(flatMessage.getDatabase())) {
      return;
    }
    if (!tables.contains(flatMessage.getTable())) {
      return;
    }
    TiTimestamp tiTimestamp = createTiTimestampFromTs(flatMessage.getEs());
    if (startTs > tiTimestamp.getVersion()) {
      return;
    }
    List<Map<String, String>> data = flatMessage.getData();
    List<Map<String, String>> old = flatMessage.getOld();
    String type = flatMessage.getType();
    switch (type) {
      case CREATE:
      case QUERY:
      case TIDB_WATERMARK:
        break;
      case INSERT:
        data.stream()
            .map(values -> toRowData(tiTimestamp, values, RowKind.INSERT))
            .forEach(out::collect);
        break;
      case DELETE:
        data.stream()
            .map(values -> toRowData(tiTimestamp, values, RowKind.DELETE))
            .forEach(out::collect);
        break;
      case UPDATE:
        old.stream()
            .map(values -> toRowData(tiTimestamp, values, RowKind.UPDATE_BEFORE))
            .forEach(out::collect);
        data.stream()
            .map(values -> toRowData(tiTimestamp, values, RowKind.UPDATE_AFTER))
            .forEach(out::collect);
        break;
      default:
        if (!ignoreParseErrors) {
          throw new IllegalArgumentException(
              String.format(
                  "Unknown \"type\" value \"%s\". The Canal message is '%s'", type, flatMessage));
        }
    }
  }

  protected RowData toRowData(TiTimestamp timestamp, Map<String, String> values, RowKind rowKind) {
    // ignore case
    values =
        values.entrySet().stream()
            .filter(e -> e.getValue() != null)
            .collect(Collectors.toMap(e -> e.getKey().toLowerCase(), Entry::getValue));
    GenericRowData rowData = new GenericRowData(rowKind, fieldNames.size() + metadata.length);
    for (int i = 0; i < fieldNames.size(); i++) {
      String name = fieldNames.get(i);
      DataType dataType = dataTypes.get(i);
      String value = values.get(name);
      rowData.setField(i, convertValue(value, dataType));
    }
    // concat metadata
    for (int i = 0; i < metadata.length; i++) {
      CDCMetadata cdcMetadata = metadata[i];
      TiDBMetadata tiDBMetadata =
          cdcMetadata
              .toTiDBMetadata()
              .orElseThrow(
                  () -> new IllegalArgumentException("Unsupported metadata: " + cdcMetadata));
      if (cdcMetadata == CDCMetadata.SOURCE_EVENT) {
        rowData.setField(fieldNames.size() + i, StringData.fromString(CDCMetadata.STREAMING));
        continue;
      }
      rowData.setField(fieldNames.size() + i, tiDBMetadata.extract(timestamp));
    }
    return rowData;
  }

  public static Object convertValue(String value, DataType dataType) {
    if (value == null) {
      return null;
    }
    LogicalType logicalType = dataType.getLogicalType();
    switch (logicalType.getTypeRoot()) {
      case BOOLEAN:
        return Boolean.parseBoolean(value);
      case TINYINT:
        return Byte.parseByte(value);
      case SMALLINT:
        return Short.parseShort(value);
      case INTEGER:
        return Integer.parseInt(value);
      case BIGINT:
        return Long.parseLong(value);
      case VARCHAR:
        return StringData.fromString(value);
      case VARBINARY:
        return value.getBytes();
      case FLOAT:
        return Float.parseFloat(value);
      case DOUBLE:
        return Double.parseDouble(value);
      case DECIMAL:
        DecimalType decimalType = (DecimalType) logicalType;
        return DecimalData.fromBigDecimal(
            new BigDecimal(value), decimalType.getPrecision(), decimalType.getScale());
      case DATE:
        return (int) LocalDate.parse(value, DATE_FORMATTER).toEpochDay();
      case TIME_WITHOUT_TIME_ZONE:
        return (int) (LocalTime.parse(value, TIME_FORMATTER).toNanoOfDay() / 1000 / 1000);
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return TimestampData.fromTimestamp(
            Timestamp.valueOf(LocalDateTime.parse(value, DATE_TIME_FORMATTER)));
      default:
        throw new IllegalStateException("Unsupported type root: " + logicalType.getTypeRoot());
    }
  }

  public static TiTimestamp createTiTimestampFromVersion(long version) {
    return new TiTimestamp(version >> 18, version & 0x3FFFF);
  }

  public static TiTimestamp createTiTimestampFromTs(long ts) {
    return new TiTimestamp(ts, 0);
  }
}
