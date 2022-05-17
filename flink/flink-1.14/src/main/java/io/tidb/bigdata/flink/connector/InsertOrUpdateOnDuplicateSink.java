package io.tidb.bigdata.flink.connector;


import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

public class InsertOrUpdateOnDuplicateSink implements DynamicTableSink {

  private final JdbcConnectorOptions jdbcOptions;
  private final JdbcExecutionOptions executionOptions;
  private final JdbcDmlOptions dmlOptions;
  private final TableSchema tableSchema;
  private final String dialectName;
  private final List<TableColumn> updateColumns;
  private final int[] index;

  public InsertOrUpdateOnDuplicateSink(
      JdbcConnectorOptions jdbcOptions,
      JdbcExecutionOptions executionOptions,
      JdbcDmlOptions dmlOptions, TableSchema tableSchema, List<TableColumn> updateColumns,
      int[] index) {
    this.jdbcOptions = jdbcOptions;
    this.executionOptions = executionOptions;
    this.dmlOptions = dmlOptions;
    this.tableSchema = tableSchema;
    this.dialectName = dmlOptions.getDialect().dialectName();
    this.updateColumns = updateColumns;
    this.index = index;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
    return ChangelogMode.newBuilder()
        .addContainedKind(RowKind.INSERT)
        .addContainedKind(RowKind.DELETE)
        .addContainedKind(RowKind.UPDATE_AFTER)
        .build();
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    final TypeInformation<RowData> rowDataTypeInformation =
        context.createTypeInformation(tableSchema.toRowDataType());
    final InsertOrDuplicateKeyUpdateOutputFormatBuilder builder = new InsertOrDuplicateKeyUpdateOutputFormatBuilder();

    builder.setJdbcOptions(jdbcOptions);
    builder.setJdbcDmlOptions(dmlOptions);
    builder.setJdbcExecutionOptions(executionOptions);
    builder.setRowDataTypeInfo(rowDataTypeInformation);
    builder.setUpdateColumnNames(
        updateColumns.stream().map(TableColumn::getName).toArray(String[]::new));
    builder.setUpdateColumnTypes(
        updateColumns.stream().map(TableColumn::getType).toArray(DataType[]::new));
    builder.setIndex(index);
    return SinkFunctionProvider.of(
        new GenericJdbcSinkFunction<>(builder.build()), jdbcOptions.getParallelism());
  }

  @Override
  public DynamicTableSink copy() {
    return new InsertOrUpdateOnDuplicateSink(jdbcOptions, executionOptions, dmlOptions, tableSchema,
        updateColumns, index);
  }

  @Override
  public String asSummaryString() {
    return "InsertOnDuplicatedKey:" + dialectName;
  }
}