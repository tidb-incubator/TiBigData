package org.tikv.bigdata.flink.tidb;

import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class TiDBRowDataInputFormat extends TiDBBaseRowDataInputFormat {

  public TiDBRowDataInputFormat(Map<String, String> properties,
      String[] fieldNames, DataType[] fieldTypes,
      TypeInformation<RowData> typeInformation) {
    super(properties, fieldNames, fieldTypes, typeInformation);
  }
}
