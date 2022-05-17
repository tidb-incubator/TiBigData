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

package io.tidb.bigdata.flink.connector;

import static org.apache.flink.table.data.RowData.createFieldGetter;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import io.tidb.bigdata.flink.DuplicateKeyUpdateOutputRowData;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat.RecordExtractor;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.executor.TableBufferReducedStatementExecutor;
import org.apache.flink.connector.jdbc.internal.executor.TableSimpleStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Builder for {@link JdbcOutputFormat} for Table/SQL.
 */
public class InsertOrDuplicateKeyUpdateOutputFormatBuilder implements Serializable {

  private static final long serialVersionUID = 1L;

  private JdbcConnectorOptions jdbcOptions;
  private JdbcExecutionOptions executionOptions;
  private JdbcDmlOptions dmlOptions;
  private TypeInformation<RowData> rowDataTypeInformation;
  private String[] updateColumnNames;
  private DataType[] updateColumnTypes;
  private int[] index;

  public InsertOrDuplicateKeyUpdateOutputFormatBuilder() {
  }

  public InsertOrDuplicateKeyUpdateOutputFormatBuilder setJdbcOptions(
      JdbcConnectorOptions jdbcOptions) {
    this.jdbcOptions = jdbcOptions;
    return this;
  }

  public InsertOrDuplicateKeyUpdateOutputFormatBuilder setJdbcExecutionOptions(
      JdbcExecutionOptions executionOptions) {
    this.executionOptions = executionOptions;
    return this;
  }

  public InsertOrDuplicateKeyUpdateOutputFormatBuilder setJdbcDmlOptions(
      JdbcDmlOptions dmlOptions) {
    this.dmlOptions = dmlOptions;
    return this;
  }

  public InsertOrDuplicateKeyUpdateOutputFormatBuilder setRowDataTypeInfo(
      TypeInformation<RowData> rowDataTypeInfo) {
    this.rowDataTypeInformation = rowDataTypeInfo;
    return this;
  }

  public InsertOrDuplicateKeyUpdateOutputFormatBuilder setUpdateColumnNames(String[] updateColumnNames) {
    this.updateColumnNames = updateColumnNames;
    return this;
  }

  public InsertOrDuplicateKeyUpdateOutputFormatBuilder setUpdateColumnTypes(DataType[] updateColumnTypes) {
    this.updateColumnTypes = updateColumnTypes;
    return this;
  }

  public InsertOrDuplicateKeyUpdateOutputFormatBuilder setIndex(int[] index) {
    this.index = index;
    return this;
  }

  public JdbcOutputFormat<RowData, ?, ?> build() {
    checkNotNull(jdbcOptions, "jdbc options can not be null");
    checkNotNull(dmlOptions, "jdbc dml options can not be null");
    checkNotNull(executionOptions, "jdbc execution options can not be null");

    final LogicalType[] logicalTypes =
        Arrays.stream(updateColumnTypes).map(DataType::getLogicalType)
            .toArray(LogicalType[]::new);

    if (dmlOptions.getKeyFields().isPresent() && dmlOptions.getKeyFields().get().length > 0) {
      // upsert query
      return new JdbcOutputFormat<>(
          new SimpleJdbcConnectionProvider(jdbcOptions),
          executionOptions,
          ctx ->
              createBufferReduceExecutor(
                  dmlOptions, logicalTypes, updateColumnNames, index),
          RecordExtractor.identity());
    } else {
      throw new IllegalStateException("Insert on duplicate key update query must have unique keys");
    }
  }

  private static JdbcBatchStatementExecutor<RowData> createBufferReduceExecutor(
      JdbcDmlOptions opt,
      LogicalType[] fieldTypes, String[] fieldNames, int[] index) {
    checkArgument(opt.getKeyFields().isPresent());
    JdbcDialect dialect = opt.getDialect();
    String tableName = opt.getTableName();
    String[] pkNames = opt.getKeyFields().get();
    int[] pkFields =
        Arrays.stream(pkNames)
            .mapToInt(Arrays.asList(opt.getFieldNames())::indexOf)
            .toArray();
    final Function<RowData, RowData> valueTransform = (rowData) -> new DuplicateKeyUpdateOutputRowData(rowData, index);

    return new TableBufferReducedStatementExecutor(
        createUpsertRowExecutor(
            dialect,
            tableName,
            fieldNames,
            fieldTypes),
        createDeleteExecutor(),
        createRowKeyExtractor(fieldTypes, pkFields),
        valueTransform);
  }

  private static JdbcBatchStatementExecutor<RowData> createUpsertRowExecutor(

      JdbcDialect dialect,
      String tableName,
      String[] fieldNames,
      LogicalType[] fieldTypes) {
    String sql = getInsertOnDuplicateKeyUpdateSql(dialect, tableName, fieldNames);
    return createSimpleRowExecutor(dialect, fieldNames, fieldTypes, sql);
  }

  private static String getInsertOnDuplicateKeyUpdateSql(
      JdbcDialect dialect,
      String tableName,
      String[] updateColumns) {
    String updateClause =
        Arrays.stream(updateColumns)
            .map(f -> dialect.quoteIdentifier(f) + "=VALUES(" + dialect.quoteIdentifier(f) + ")")
            .collect(Collectors.joining(", "));
    return dialect.getInsertIntoStatement(tableName, updateColumns)
        + " ON DUPLICATE KEY UPDATE "
        + updateClause;
  }

  private static JdbcBatchStatementExecutor<RowData> createDeleteExecutor() {
    return new JdbcBatchStatementExecutor<RowData>() {
      @Override
      public void prepareStatements(Connection connection) throws SQLException {
      }

      @Override
      public void addToBatch(RowData record) throws SQLException {
        throw new UnsupportedOperationException();
      }

      @Override
      public void executeBatch() throws SQLException {
      }

      @Override
      public void closeStatements() throws SQLException {
      }
    };
  }

  private static JdbcBatchStatementExecutor<RowData> createSimpleRowExecutor(
      JdbcDialect dialect, String[] fieldNames, LogicalType[] fieldTypes, final String sql) {
    final JdbcRowConverter rowConverter = dialect.getRowConverter(RowType.of(fieldTypes));
    return new TableSimpleStatementExecutor(
        connection ->
            FieldNamedPreparedStatement.prepareStatement(connection, sql, fieldNames),
        rowConverter);
  }

  private static Function<RowData, RowData> createRowKeyExtractor(
      LogicalType[] logicalTypes, int[] pkFields) {
    final FieldGetter[] fieldGetters = new FieldGetter[pkFields.length];
    for (int i = 0; i < pkFields.length; i++) {
      fieldGetters[i] = createFieldGetter(logicalTypes[pkFields[i]], pkFields[i]);
    }
    return row -> getPrimaryKey(row, fieldGetters);
  }

  private static RowData getPrimaryKey(RowData row, FieldGetter[] fieldGetters) {
    GenericRowData pkRow = new GenericRowData(fieldGetters.length);
    for (int i = 0; i < fieldGetters.length; i++) {
      pkRow.setField(i, fieldGetters[i].getFieldOrNull(row));
    }
    return pkRow;
  }
}
