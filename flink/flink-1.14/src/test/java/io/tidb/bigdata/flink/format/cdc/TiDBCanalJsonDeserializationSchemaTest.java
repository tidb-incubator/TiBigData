/*
 * Copyright 2022 TiDB Project Authors.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

public class TiDBCanalJsonDeserializationSchemaTest {

  @SuppressWarnings("unchecked")
  private TiDBCanalJsonDeserializationSchema createDeserializationSchema() {
    DataType physicalDataType =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT()), DataTypes.FIELD("name", DataTypes.STRING()));
    Set<String> schemas = ImmutableSet.of("test");
    Set<String> tables = ImmutableSet.of("test_cdc");
    TypeInformation<RowData> producedTypeInfo =
        (TypeInformation<RowData>)
            ScanRuntimeProviderContext.INSTANCE.createTypeInformation(physicalDataType);
    return new TiDBCanalJsonDeserializationSchema(
        physicalDataType,
        schemas,
        tables,
        new CDCMetadata[0],
        0,
        producedTypeInfo,
        false,
        TimestampFormat.SQL);
  }

  @Test
  public void testIgnoreCase() throws IOException {
    TiDBCanalJsonDeserializationSchema deserializationSchema = createDeserializationSchema();
    JsonRowDataDeserializationSchema jsonDeserializer = deserializationSchema.getJsonDeserializer();
    List<String> fieldNames = deserializationSchema.getFieldNames();

    Assert.assertEquals(ImmutableList.of("id", "name"), fieldNames);

    ListCollector collector = new ListCollector();

    String json =
        "{\"id\":0,\"database\":\"test\",\"table\":\"test_Cdc\",\"pkNames\":[\"id\"],\"isDdl\":false,\"type\":\"INSERT\",\"es\":1668665579405,\"ts\":1668665582384,\"sql\":\"\",\"sqlType\":{\"Name\":12,\"id\":-5},\"mysqlType\":{\"Name\":\"varchar\",\"id\":\"bigint\"},\"data\":[{\"Name\":\"zs\",\"id\":\"1\"}],\"old\":null,\"_tidb\":{\"commitTs\":437430669647544322}}";
    deserializationSchema.deserialize(json.getBytes(), collector);
    RowData rowData = collector.getRows().get(0);
    GenericRowData genericRowData = new GenericRowData(2);
    genericRowData.setField(0, 1L);
    genericRowData.setField(1, StringData.fromString("zs"));
    genericRowData.setRowKind(RowKind.INSERT);
    Assert.assertEquals(genericRowData, rowData);

    // jsonDeserializer should be changed if columns are not match lowercase
    Assert.assertEquals(ImmutableList.of("id", "Name"), fieldNames);
    Assert.assertNotSame(jsonDeserializer, deserializationSchema.getJsonDeserializer());
    jsonDeserializer = deserializationSchema.getJsonDeserializer();

    collector.getRows().clear();

    // jsonDeserializer should be only changed once
    deserializationSchema.deserialize(json.getBytes(), collector);
    Assert.assertEquals(ImmutableList.of("id", "Name"), fieldNames);
    Assert.assertSame(jsonDeserializer, deserializationSchema.getJsonDeserializer());
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
