/*
 * Copyright 2020 Zhihu.
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

package com.zhihu.tibigdata.flink.tidb;

import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;

public class TiDBDynamicTableSource implements ScanTableSource, SupportsLimitPushDown,
    SupportsProjectionPushDown {

  private final TableSchema tableSchema;

  private final Map<String, String> properties;

  private long limit = Long.MAX_VALUE;

  private int[][] projectedFields;

  public TiDBDynamicTableSource(TableSchema tableSchema, Map<String, String> properties) {
    this.tableSchema = tableSchema;
    this.properties = properties;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
    TypeInformation<RowData> typeInformation = runtimeProviderContext
        .createTypeInformation(tableSchema.toRowDataType());
    TiDBRowDataInputFormat tidbRowDataInputFormat = new TiDBRowDataInputFormat(properties,
        tableSchema.getFieldNames(), tableSchema.getFieldDataTypes(), typeInformation);
    tidbRowDataInputFormat.setLimit(limit);
    if (projectedFields != null) {
      tidbRowDataInputFormat.setProjectedFields(projectedFields);
    }
    return InputFormatProvider.of(tidbRowDataInputFormat);
  }

  @Override
  public DynamicTableSource copy() {
    TiDBDynamicTableSource tableSource = new TiDBDynamicTableSource(tableSchema, properties);
    tableSource.limit = this.limit;
    tableSource.projectedFields = this.projectedFields;
    return tableSource;
  }

  @Override
  public String asSummaryString() {
    return this.getClass().getName();
  }

  @Override
  public void applyLimit(long limit) {
    this.limit = limit;
  }

  @Override
  public boolean supportsNestedProjection() {
    return false;
  }

  @Override
  public void applyProjection(int[][] projectedFields) {
    this.projectedFields = projectedFields;
  }
}
