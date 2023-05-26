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

import static io.tidb.bigdata.flink.format.canal.CanalDeserializationSchema.DATE_FORMATTER;
import static io.tidb.bigdata.flink.format.canal.CanalDeserializationSchema.TIME_FORMATTER;

import com.google.common.collect.ImmutableSet;
import io.tidb.bigdata.flink.format.cdc.CDCMetadata;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

public class CanalDeserializationSchemaTest {

  public static final DataType TYPES =
      DataTypes.ROW(
          DataTypes.FIELD("c1", DataTypes.TINYINT()),
          DataTypes.FIELD("c2", DataTypes.SMALLINT()),
          DataTypes.FIELD("c3", DataTypes.INT()),
          DataTypes.FIELD("c4", DataTypes.INT()),
          DataTypes.FIELD("c5", DataTypes.BIGINT()),
          DataTypes.FIELD("c6", DataTypes.STRING()),
          DataTypes.FIELD("c7", DataTypes.STRING()),
          DataTypes.FIELD("c8", DataTypes.STRING()),
          DataTypes.FIELD("c9", DataTypes.STRING()),
          DataTypes.FIELD("c10", DataTypes.STRING()),
          DataTypes.FIELD("c11", DataTypes.STRING()),
          DataTypes.FIELD("c12", DataTypes.BYTES()),
          DataTypes.FIELD("c13", DataTypes.BYTES()),
          DataTypes.FIELD("c14", DataTypes.BYTES()),
          DataTypes.FIELD("c15", DataTypes.BYTES()),
          DataTypes.FIELD("c16", DataTypes.BYTES()),
          DataTypes.FIELD("c17", DataTypes.BYTES()),
          DataTypes.FIELD("c18", DataTypes.FLOAT()),
          DataTypes.FIELD("c19", DataTypes.DOUBLE()),
          DataTypes.FIELD("c20", DataTypes.DECIMAL(6, 3)),
          DataTypes.FIELD("c21", DataTypes.DATE()),
          DataTypes.FIELD("c22", DataTypes.TIME(0)),
          DataTypes.FIELD("c23", DataTypes.TIMESTAMP(6)),
          DataTypes.FIELD("c24", DataTypes.TIMESTAMP(6)),
          DataTypes.FIELD("c25", DataTypes.INT()),
          DataTypes.FIELD("c26", DataTypes.TINYINT()),
          DataTypes.FIELD("c27", DataTypes.STRING()),
          DataTypes.FIELD("c28", DataTypes.STRING()),
          DataTypes.FIELD("c29", DataTypes.STRING()));

  public static final byte[] JSON_DATA =
      "{\"id\":0,\"database\":\"test\",\"table\":\"test_tidb_type\",\"pkNames\":[\"c1\"],\"isDdl\":false,\"type\":\"INSERT\",\"es\":1685085356868,\"ts\":1685085357997,\"sql\":\"\",\"sqlType\":{\"c1\":-6,\"c10\":2005,\"c11\":2005,\"c12\":2004,\"c13\":2004,\"c14\":2004,\"c15\":2004,\"c16\":2004,\"c17\":2004,\"c18\":7,\"c19\":8,\"c2\":5,\"c20\":3,\"c21\":91,\"c22\":92,\"c23\":93,\"c24\":93,\"c25\":12,\"c26\":-6,\"c27\":12,\"c28\":4,\"c29\":-7,\"c3\":4,\"c4\":4,\"c5\":-5,\"c6\":1,\"c7\":12,\"c8\":2005,\"c9\":2005},\"mysqlType\":{\"c1\":\"tinyint\",\"c10\":\"text\",\"c11\":\"longtext\",\"c12\":\"binary\",\"c13\":\"varbinary\",\"c14\":\"tinyblob\",\"c15\":\"mediumblob\",\"c16\":\"blob\",\"c17\":\"longblob\",\"c18\":\"float\",\"c19\":\"double\",\"c2\":\"smallint\",\"c20\":\"decimal\",\"c21\":\"date\",\"c22\":\"time\",\"c23\":\"datetime\",\"c24\":\"timestamp\",\"c25\":\"year\",\"c26\":\"tinyint\",\"c27\":\"json\",\"c28\":\"enum\",\"c29\":\"set\",\"c3\":\"mediumint\",\"c4\":\"int\",\"c5\":\"bigint\",\"c6\":\"char\",\"c7\":\"varchar\",\"c8\":\"tinytext\",\"c9\":\"mediumtext\"},\"data\":[{\"c1\":\"1\",\"c10\":\"texttype\",\"c11\":\"longtexttype\",\"c12\":\"binarytype\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\",\"c13\":\"varbinarytype\",\"c14\":\"tinyblobtype\",\"c15\":\"mediumblobtype\",\"c16\":\"blobtype\",\"c17\":\"longblobtype\",\"c18\":\"1.2339999675750732\",\"c19\":\"2.456789\",\"c2\":\"2\",\"c20\":\"123.456\",\"c21\":\"2023-05-26\",\"c22\":\"15:15:56\",\"c23\":\"2023-05-26 15:15:56\",\"c24\":\"2023-05-26 15:15:56\",\"c25\":\"2020\",\"c26\":\"1\",\"c27\":\"{\\\"a\\\": 1, \\\"b\\\": 2}\",\"c28\":\"1\",\"c29\":\"1\",\"c3\":\"3\",\"c4\":\"4\",\"c5\":\"5\",\"c6\":\"chartype\",\"c7\":\"varchartype\",\"c8\":\"tinytexttype\",\"c9\":\"mediumtexttype\"}],\"old\":null}"
          .getBytes();
  public static final RowData JSON_DECODE_RESULT =
      GenericRowData.ofKind(
          RowKind.INSERT,
          (byte) 1,
          (short) 2,
          3,
          4,
          5L,
          StringData.fromString("chartype"),
          StringData.fromString("varchartype"),
          StringData.fromString("tinytexttype"),
          StringData.fromString("mediumtexttype"),
          StringData.fromString("texttype"),
          StringData.fromString("longtexttype"),
          ("binarytype" + new String(new byte[10])).getBytes(),
          "varbinarytype".getBytes(),
          "tinyblobtype".getBytes(),
          "mediumblobtype".getBytes(),
          "blobtype".getBytes(),
          "longblobtype".getBytes(),
          1.234F,
          2.456789,
          DecimalData.fromBigDecimal(new BigDecimal("123.456"), 6, 3),
          (int) LocalDate.parse("2023-05-26", DATE_FORMATTER).toEpochDay(),
          (int) (LocalTime.parse("15:15:56", TIME_FORMATTER).toNanoOfDay() / 1000 / 1000),
          TimestampData.fromLocalDateTime(LocalDateTime.of(2023, 5, 26, 15, 15, 56)),
          TimestampData.fromLocalDateTime(LocalDateTime.of(2023, 5, 26, 15, 15, 56)),
          2020,
          (byte) 1,
          StringData.fromString("{\"a\": 1, \"b\": 2}"),
          StringData.fromString("1"),
          StringData.fromString("1"),
          441735015790804992L,
          TimestampData.fromEpochMillis(441735015790804992L >> 18),
          StringData.fromString(CDCMetadata.STREAMING));

  public static final byte[] PROTOBUF_DATA =
      Base64.getDecoder()
          .decode(
              "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");

  public static final RowData PROTOBUF_DECODE_RESULT0 =
      GenericRowData.ofKind(
          RowKind.INSERT,
          (byte) 1,
          (short) 2,
          3,
          4,
          5L,
          StringData.fromString("chartype"),
          StringData.fromString("varchartype"),
          StringData.fromString("tinytexttype"),
          StringData.fromString("mediumtexttype"),
          StringData.fromString("texttype"),
          StringData.fromString("longtexttype"),
          ("binarytype" + new String(new byte[10])).getBytes(),
          "varbinarytype".getBytes(),
          "tinyblobtype".getBytes(),
          "mediumblobtype".getBytes(),
          "blobtype".getBytes(),
          "longblobtype".getBytes(),
          1.234F,
          2.456789,
          DecimalData.fromBigDecimal(new BigDecimal("123.456"), 6, 3),
          (int) LocalDate.parse("2023-05-26", DATE_FORMATTER).toEpochDay(),
          (int) (LocalTime.parse("16:11:24", TIME_FORMATTER).toNanoOfDay() / 1000 / 1000),
          TimestampData.fromLocalDateTime(LocalDateTime.of(2023, 5, 26, 16, 11, 24)),
          TimestampData.fromLocalDateTime(LocalDateTime.of(2023, 5, 26, 16, 11, 24)),
          2020,
          (byte) 1,
          StringData.fromString("{\"a\": 1, \"b\": 2}"),
          StringData.fromString("1"),
          StringData.fromString("1"),
          441735888086761472L,
          TimestampData.fromEpochMillis(441735888086761472L >> 18),
          StringData.fromString(CDCMetadata.STREAMING));

  public static final RowData PROTOBUF_DECODE_RESULT1 =
      GenericRowData.ofKind(
          RowKind.INSERT,
          (byte) 2,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          441735888086761472L,
          TimestampData.fromEpochMillis(441735888086761472L >> 18),
          StringData.fromString(CDCMetadata.STREAMING));

  public static final Set<String> SCHEMAS = ImmutableSet.of("test");

  public static final Set<String> TABLES = ImmutableSet.of("test_tidb_type");

  public static final CDCMetadata[] METADATA =
      new CDCMetadata[] {
        CDCMetadata.COMMIT_VERSION, CDCMetadata.COMMIT_TIMESTAMP, CDCMetadata.SOURCE_EVENT
      };

  @Test
  public void testJsonDecode() throws IOException {
    ListCollector collector = new ListCollector();
    CanalJsonDeserializationSchema canalJsonDeserializationSchema =
        new CanalJsonDeserializationSchema(TYPES, SCHEMAS, TABLES, METADATA, 0, null, false);
    canalJsonDeserializationSchema.deserialize(JSON_DATA, collector);
    RowData row = collector.getRows().get(0);
    Assert.assertEquals(row, JSON_DECODE_RESULT);
  }

  @Test
  public void testProtobufDecode() throws IOException {
    ListCollector collector = new ListCollector();
    CanalProtobufDeserializationSchema canalProtobufDeserializationSchema =
        new CanalProtobufDeserializationSchema(TYPES, SCHEMAS, TABLES, METADATA, 0, null, false);
    canalProtobufDeserializationSchema.deserialize(PROTOBUF_DATA, collector);
    Assert.assertEquals(PROTOBUF_DECODE_RESULT0, collector.getRows().get(0));
    Assert.assertEquals(PROTOBUF_DECODE_RESULT1, collector.getRows().get(1));
  }

  public static class ListCollector implements Collector<RowData> {

    private final List<RowData> rows = new ArrayList<>();

    @Override
    public void collect(RowData record) {
      rows.add(record);
    }

    @Override
    public void close() {}

    public List<RowData> getRows() {
      return rows;
    }
  }
}
