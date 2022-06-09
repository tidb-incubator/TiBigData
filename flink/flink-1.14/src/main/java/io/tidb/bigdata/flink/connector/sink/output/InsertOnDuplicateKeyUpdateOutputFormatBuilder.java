/*
 * Copyright 2022 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.tidb.bigdata.flink.connector.sink.output;

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat.RecordExtractor;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.executor.TableBufferedStatementExecutor;
import org.apache.flink.connector.jdbc.internal.executor.TableSimpleStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/** Builder for {@link JdbcOutputFormat} for clause `INSERT ... ON DUPLICATE KEY UPDATE`. */
public class InsertOnDuplicateKeyUpdateOutputFormatBuilder implements Serializable {

  private static final long serialVersionUID = 1L;

  private JdbcConnectorOptions jdbcOptions;
  private JdbcExecutionOptions executionOptions;
  private JdbcDmlOptions dmlOptions;
  private String[] updateColumnNames;
  private DataType[] updateColumnTypes;
  private int[] updateColumnIndexes;

  public InsertOnDuplicateKeyUpdateOutputFormatBuilder() {}

  public InsertOnDuplicateKeyUpdateOutputFormatBuilder setJdbcOptions(
      JdbcConnectorOptions jdbcOptions) {
    this.jdbcOptions = jdbcOptions;
    return this;
  }

  public InsertOnDuplicateKeyUpdateOutputFormatBuilder setJdbcExecutionOptions(
      JdbcExecutionOptions executionOptions) {
    this.executionOptions = executionOptions;
    return this;
  }

  public InsertOnDuplicateKeyUpdateOutputFormatBuilder setJdbcDmlOptions(
      JdbcDmlOptions dmlOptions) {
    this.dmlOptions = dmlOptions;
    return this;
  }

  public InsertOnDuplicateKeyUpdateOutputFormatBuilder setUpdateColumnNames(
      String[] updateColumnNames) {
    this.updateColumnNames = updateColumnNames;
    return this;
  }

  public InsertOnDuplicateKeyUpdateOutputFormatBuilder setUpdateColumnTypes(
      DataType[] updateColumnTypes) {
    this.updateColumnTypes = updateColumnTypes;
    return this;
  }

  public InsertOnDuplicateKeyUpdateOutputFormatBuilder setUpdateColumnIndexes(
      int[] updateColumnIndexes) {
    this.updateColumnIndexes = updateColumnIndexes;
    return this;
  }

  public JdbcOutputFormat<RowData, ?, ?> build() {
    checkNotNull(jdbcOptions, "jdbc options can not be null");
    checkNotNull(dmlOptions, "jdbc dml options can not be null");
    checkNotNull(executionOptions, "jdbc execution options can not be null");
    checkNotNull(updateColumnNames, "updateColumnNames options can not be null");
    checkNotNull(updateColumnTypes, "updateColumnTypes options can not be null");
    checkNotNull(updateColumnIndexes, "updateColumnIndexes options can not be null");

    final LogicalType[] logicalTypes =
        Arrays.stream(updateColumnTypes).map(DataType::getLogicalType).toArray(LogicalType[]::new);

    // upsert query
    return new JdbcOutputFormat<>(
        new SimpleJdbcConnectionProvider(jdbcOptions),
        executionOptions,
        ctx ->
            createSimpleBufferedExecutor(
                dmlOptions, logicalTypes, updateColumnNames, updateColumnIndexes),
        RecordExtractor.identity());
  }

  private static JdbcBatchStatementExecutor<RowData> createSimpleBufferedExecutor(
      JdbcDmlOptions opt,
      LogicalType[] updateColumnTypes,
      String[] updateColumnNames,
      int[] updateColumnIndexes) {
    JdbcDialect dialect = opt.getDialect();
    String tableName = opt.getTableName();
    // use wrapper class to prune column
    final Function<RowData, RowData> valueTransform =
        (rowData) -> new ColumnPruningOutputRowData(rowData, updateColumnIndexes);

    return new TableBufferedStatementExecutor(
        createInsertOnDuplicateUpdateRowExecutor(
            dialect, tableName, updateColumnNames, updateColumnTypes),
        valueTransform);
  }

  private static JdbcBatchStatementExecutor<RowData> createInsertOnDuplicateUpdateRowExecutor(
      JdbcDialect dialect,
      String tableName,
      String[] updateColumnNames,
      LogicalType[] updateColumnTypes) {
    String sql = getInsertOnDuplicateKeyUpdateSql(dialect, tableName, updateColumnNames);
    return createSimpleRowExecutor(dialect, updateColumnNames, updateColumnTypes, sql);
  }

  private static String getInsertOnDuplicateKeyUpdateSql(
      JdbcDialect dialect, String tableName, String[] updateColumns) {
    String updateClause =
        Arrays.stream(updateColumns)
            .map(f -> dialect.quoteIdentifier(f) + "=VALUES(" + dialect.quoteIdentifier(f) + ")")
            .collect(Collectors.joining(", "));
    return dialect.getInsertIntoStatement(tableName, updateColumns)
        + " ON DUPLICATE KEY UPDATE "
        + updateClause;
  }

  private static JdbcBatchStatementExecutor<RowData> createSimpleRowExecutor(
      JdbcDialect dialect, String[] fieldNames, LogicalType[] fieldTypes, final String sql) {
    final JdbcRowConverter rowConverter = dialect.getRowConverter(RowType.of(fieldTypes));
    return new TableSimpleStatementExecutor(
        connection -> FieldNamedPreparedStatement.prepareStatement(connection, sql, fieldNames),
        rowConverter);
  }
}
