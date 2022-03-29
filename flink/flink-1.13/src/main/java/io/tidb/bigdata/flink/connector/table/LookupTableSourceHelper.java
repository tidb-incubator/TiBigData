/*
 * Copyright 2021 TiDB Project Authors.
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

package io.tidb.bigdata.flink.connector.table;

import java.util.Arrays;
import java.util.Map;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.table.JdbcRowDataLookupFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.LookupTableSource.LookupContext;
import org.apache.flink.table.connector.source.LookupTableSource.LookupRuntimeProvider;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

public class LookupTableSourceHelper {

  private final JdbcLookupOptions lookupOptions;

  public LookupTableSourceHelper(
      JdbcLookupOptions lookupOptions) {
    this.lookupOptions = lookupOptions;
  }

  public LookupRuntimeProvider getLookupRuntimeProvider(
      ResolvedCatalogTable table, LookupContext context) {

    String[] keyNames = new String[context.getKeys().length];
    TableSchema physicalSchema =
        TableSchemaUtils.getPhysicalSchema(table.getSchema());
    Map<String, String> properties = table.getOptions();
    for (int i = 0; i < keyNames.length; i++) {
      int[] innerKeyArr = context.getKeys()[i];
      Preconditions.checkArgument(
          innerKeyArr.length == 1, "JDBC only support non-nested look up keys");
      keyNames[i] = physicalSchema.getFieldNames()[innerKeyArr[0]];
    }
    final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
    final AsyncLookupOptions asyncLookupOptions = JdbcUtils.getAsyncJdbcOptions(properties);
    if (asyncLookupOptions.isAsync()) {
      return AsyncTableFunctionProvider.of(
          new AsyncJdbcLookUpFunction(
              JdbcUtils.getJdbcOptions(properties),
              asyncLookupOptions,
              physicalSchema.getFieldNames(),
              physicalSchema.getFieldDataTypes(),
              keyNames,
              rowType));
    } else {
      return TableFunctionProvider.of(
          new JdbcRowDataLookupFunction(
              JdbcUtils.getJdbcOptions(properties),
              lookupOptions,
              physicalSchema.getFieldNames(),
              physicalSchema.getFieldDataTypes(),
              keyNames,
              rowType));
    }

  }
}