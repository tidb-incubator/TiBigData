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
package com.zhihu.flink.tidb.source;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

public class TiDBTableSource extends InputFormatTableSource<Row> {

  private final TableSchema tableSchema;

  private final String pdAddresses;

  private final String databaseName;

  private final String tableName;

  private final InputFormat<Row, ?> inputFormat;

  /**
   * see {@link TiDBTableSource#builder()}
   *
   * @param tableSchema
   * @param pdAddresses
   * @param databaseName
   * @param tableName
   */
  private TiDBTableSource(TableSchema tableSchema, String pdAddresses, String databaseName, String tableName) {

    this.tableSchema = tableSchema;
    this.pdAddresses = pdAddresses;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.inputFormat = TiDBInputFormat.builder()
        .setPdAddresses(this.pdAddresses)
        .setDatabaseName(this.databaseName)
        .setTableName(this.tableName)
        .setFieldNames(this.tableSchema.getFieldNames())
        .setFieldTypes(this.tableSchema.getFieldDataTypes())
        .build();
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

    private TableSchema tableSchema;

    private String pdAddresses;

    private String databaseName;

    private String tableName;

    private Builder() {

    }

    public Builder setPdAddresses(String pdAddresses) {
      this.pdAddresses = pdAddresses;
      return this;
    }

    public Builder setDatabaseName(String databaseName) {
      this.databaseName = databaseName;
      return this;
    }

    public Builder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder setTableSchema(TableSchema tableSchema) {
      this.tableSchema = tableSchema;
      return this;
    }

    public TiDBTableSource build() {
      return new TiDBTableSource(tableSchema, pdAddresses, databaseName, tableName);
    }
  }
}
