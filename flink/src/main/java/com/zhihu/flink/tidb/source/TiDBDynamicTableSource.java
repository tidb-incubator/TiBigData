package com.zhihu.flink.tidb.source;

import java.util.Properties;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;

public class TiDBDynamicTableSource implements ScanTableSource {

  private final TableSchema tableSchema;

  private final Properties properties;

  public TiDBDynamicTableSource(TableSchema tableSchema, Properties properties) {
    this.tableSchema = tableSchema;
    this.properties = properties;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  @SuppressWarnings("unchecked")
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
    TypeInformation<RowData> typeInformation = (TypeInformation<RowData>) runtimeProviderContext
        .createTypeInformation(tableSchema.toRowDataType());
    TiDBRowDataInputFormat tidbRowDataInputFormat = new TiDBRowDataInputFormat(properties,
        tableSchema.getFieldNames(), tableSchema.getFieldDataTypes(), typeInformation);
    return InputFormatProvider.of(tidbRowDataInputFormat);
  }

  @Override
  public DynamicTableSource copy() {
    return new TiDBDynamicTableSource(tableSchema, properties);
  }

  @Override
  public String asSummaryString() {
    return this.getClass().getName();
  }
}
