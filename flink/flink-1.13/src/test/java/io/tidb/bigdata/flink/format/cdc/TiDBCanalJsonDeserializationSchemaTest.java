package io.tidb.bigdata.flink.format.cdc;


import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
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

  private final DataType physicalDataType = DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT()),
      DataTypes.FIELD("name", DataTypes.STRING()));
  private final Set<String> schemas = ImmutableSet.of("test");
  private final Set<String> tables = ImmutableSet.of("test_cdc");
  private final CDCMetadata[] metadata = new CDCMetadata[0];
  private final long startTs = 0L;

  @SuppressWarnings("unchecked")
  private TiDBCanalJsonDeserializationSchema createDeserializationSchema() {
    TypeInformation<RowData> producedTypeInfo = (TypeInformation<RowData>)
        ScanRuntimeProviderContext.INSTANCE.createTypeInformation(physicalDataType);
    return new TiDBCanalJsonDeserializationSchema(
        physicalDataType, schemas, tables, metadata, startTs, producedTypeInfo, false,
        TimestampFormat.SQL);
  }


  @Test
  public void testIgnoreCase() throws IOException {
    TiDBCanalJsonDeserializationSchema deserializationSchema = createDeserializationSchema();
    String json = "{\"id\":0,\"database\":\"test\",\"table\":\"test_Cdc\",\"pkNames\":[\"id\"],\"isDdl\":false,\"type\":\"INSERT\",\"es\":1668664153755,\"ts\":1668664155950,\"sql\":\"\",\"sqlType\":{\"Name\":12,\"id\":-5},\"mysqlType\":{\"Name\":\"varchar\",\"id\":\"bigint\"},\"data\":[{\"Name\":\"zs\",\"id\":\"27\"}],\"old\":null,\"_tidb\":{\"commitTs\":437430295921950722}}";
    ListCollector collector = new ListCollector();
    deserializationSchema.deserialize(json.getBytes(), collector);
    RowData rowData = collector.getRows().get(0);
    GenericRowData genericRowData = new GenericRowData(2);
    genericRowData.setField(0, 27L);
    genericRowData.setField(1, StringData.fromString("zs"));
    genericRowData.setRowKind(RowKind.INSERT);
    Assert.assertEquals(genericRowData, rowData);
  }


  public static class ListCollector implements Collector<RowData> {

    private final List<RowData> rows = new ArrayList<>();

    @Override
    public void collect(RowData record) {
      rows.add(record);
    }

    @Override
    public void close() {

    }

    public List<RowData> getRows() {
      return rows;
    }
  }

}