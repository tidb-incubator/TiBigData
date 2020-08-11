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

package com.zhihu.tibigdata.flink.tidb.source;

import java.util.Properties;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

public class TiDBTableSource extends InputFormatTableSource<Row> {

  private final TableSchema tableSchema;

  private final Properties properties;

  private final InputFormat<Row, ?> inputFormat;

  public TiDBTableSource(TableSchema tableSchema, Properties properties) {
    this.tableSchema = tableSchema;
    this.properties = properties;
    this.inputFormat = new TiDBInputFormat(this.properties, tableSchema.getFieldNames(),
        tableSchema.getFieldDataTypes());
  }

  @Override
  public DataType getProducedDataType() {
    return tableSchema.toRowDataType();
  }

  @Override
  public InputFormat<Row, ?> getInputFormat() {
    return inputFormat;
  }

  @Override
  public TableSchema getTableSchema() {
    return tableSchema;
  }
}
