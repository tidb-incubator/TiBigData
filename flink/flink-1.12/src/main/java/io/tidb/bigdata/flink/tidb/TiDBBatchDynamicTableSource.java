/*
 * Copyright 2020 TiDB Project Authors.
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

package io.tidb.bigdata.flink.tidb;

import java.util.Map;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;

public class TiDBBatchDynamicTableSource extends TiDBDynamicTableSource implements
    SupportsLimitPushDown, SupportsProjectionPushDown {

  protected long limit = Long.MAX_VALUE;

  protected int[][] projectedFields = null;

  public TiDBBatchDynamicTableSource(
      TableSchema tableSchema,
      Map<String, String> properties,
      JdbcLookupOptions lookupOptions) {
    super(tableSchema, properties, lookupOptions);
  }

  protected TiDBRowDataInputFormat getInputFormat(ScanContext context) {
    TiDBRowDataInputFormat inputFormat = super.getInputFormat(context, 0);
    inputFormat.setLimit(limit);
    if (projectedFields != null) {
      inputFormat.setProjectedFields(projectedFields);
    }
    return inputFormat;
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
    return InputFormatProvider.of(getInputFormat(context));
  }

  @Override
  public DynamicTableSource copy() {
    TiDBBatchDynamicTableSource tableSource =
        new TiDBBatchDynamicTableSource(tableSchema, properties, lookupOptions);
    tableSource.limit = this.limit;
    tableSource.projectedFields = this.projectedFields;
    copyTo(tableSource);
    return tableSource;
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

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }
}
