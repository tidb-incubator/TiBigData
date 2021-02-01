/*
 * Copyright 2020 TiKV Project Authors.
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

package org.tikv.bigdata.flink.tidb;

import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiDBDynamicTableSource extends TiDBBaseDynamicTableSource {

  static final Logger LOG = LoggerFactory.getLogger(TiDBDynamicTableSource.class);

  public TiDBDynamicTableSource(TableSchema tableSchema, Map<String, String> properties) {
    super(tableSchema, properties);
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
}
