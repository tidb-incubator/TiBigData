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
package com.zhihu.flink.source.tidb;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

public class TiDBTableSource extends InputFormatTableSource<Row> {

  private TableSchema tableSchema;

  private String pdAddresses;

  private String databaseName;

  private String tableName;

  private InputFormat<Row, ?> inputFormat;

  /**
   * see {@link TiDBTableSource#builder()}
   */
  private TiDBTableSource() {

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

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private TiDBTableSource source;

    private Builder() {
      source = new TiDBTableSource();
    }

    public Builder setPdAddresses(String pdAddresses) {
      source.pdAddresses = pdAddresses;
      return this;
    }

    public Builder setDatabaseName(String databaseName) {
      source.databaseName = databaseName;
      return this;
    }

    public Builder setTableName(String tableName) {
      source.tableName = tableName;
      return this;
    }

    public Builder setTableSchema(TableSchema tableSchema) {
      source.tableSchema = tableSchema;
      return this;
    }

    public TiDBTableSource build() {
      source.inputFormat = TiDBInputFormat.builder()
          .setPdAddresses(source.pdAddresses)
          .setDatabaseName(source.databaseName)
          .setTableName(source.tableName)
          .setFieldNames(source.tableSchema.getFieldNames())
          .setFieldTypes(source.tableSchema.getFieldDataTypes())
          .build();
      return source;
    }
  }
}
