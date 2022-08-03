/*
 * Copyright 2022 TiDB Project Authors.
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

package io.tidb.bigdata.flink.connector.sink;

import static io.tidb.bigdata.flink.connector.TiDBOptions.SinkTransaction.CHECKPOINT;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SinkTransaction.GLOBAL;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SinkTransaction.MINIBATCH;

import io.tidb.bigdata.flink.connector.TiDBOptions.SinkTransaction;
import io.tidb.bigdata.flink.connector.sink.function.TiDBKeyedProcessFunctionFactory;
import io.tidb.bigdata.flink.connector.sink.function.TiDBSinkFunction;
import io.tidb.bigdata.flink.connector.sink.operator.TiDBCommitOperator;
import io.tidb.bigdata.flink.connector.sink.operator.TiDBGlobalWriteOperator;
import io.tidb.bigdata.flink.connector.sink.operator.TiDBMiniBatchWriteOperator;
import io.tidb.bigdata.flink.connector.sink.operator.TiDBWriteOperator;
import io.tidb.bigdata.flink.connector.sink.serializer.TiDBTransactionContextSerializer;
import io.tidb.bigdata.flink.connector.sink.serializer.TiDBTransactionStateSerializer;
import io.tidb.bigdata.flink.connector.utils.TiDBRowConverter;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.SqlUtils;
import io.tidb.bigdata.tidb.TiDBWriteHelper;
import io.tidb.bigdata.tidb.TiDBWriteMode;
import io.tidb.bigdata.tidb.meta.TiColumnInfo;
import io.tidb.bigdata.tidb.meta.TiIndexColumn;
import io.tidb.bigdata.tidb.meta.TiIndexInfo;
import io.tidb.bigdata.tidb.meta.TiTableInfo;
import io.tidb.bigdata.tidb.row.Row;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink.Context;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.BytePairWrapper;
import org.tikv.common.meta.TiTimestamp;

public class TiDBDataStreamSinkProvider implements DataStreamSinkProvider {

  private static final Logger LOG = LoggerFactory.getLogger(TiDBDataStreamSinkProvider.class);
  public static final String PRIMARY_KEY_PREFIX = "TIBIGDATA_PRIMARY_KEY_";

  private final ResolvedCatalogTable table;
  private final Context context;
  private final String databaseName;
  private final String tableName;
  private final Map<String, String> properties;
  private final TableSchema tableSchema;
  private final TiDBSinkOptions sinkOptions;

  public TiDBDataStreamSinkProvider(
      String databaseName,
      String tableName,
      ResolvedCatalogTable table,
      Context context,
      TiDBSinkOptions sinkOptions) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.table = table;
    this.context = context;
    this.properties = table.getOptions();
    this.tableSchema = TableSchemaUtils.getPhysicalSchema(table.getSchema());
    this.sinkOptions = sinkOptions;
  }

  private byte[] fakePrimaryKey() {
    return (PRIMARY_KEY_PREFIX + UUID.randomUUID()).getBytes();
  }

  private DataStream<Row> deduplicate(DataStream<Row> tiRowDataStream, TiTableInfo tiTableInfo) {
    List<TiIndexInfo> uniqueIndexes =
        SqlUtils.getUniqueIndexes(tiTableInfo, sinkOptions.isIgnoreAutoincrementColumn());
    if (uniqueIndexes.size() == 0) {
      return tiRowDataStream;
    }

    for (TiIndexInfo uniqueIndex : uniqueIndexes) {
      List<Integer> columnIndexes =
          uniqueIndex.getIndexColumns().stream()
              .map(TiIndexColumn::getOffset)
              .collect(Collectors.toList());
      List<String> uniqueIndexColumnNames =
          uniqueIndex.getIndexColumns().stream()
              .map(TiIndexColumn::getName)
              .collect(Collectors.toList());

      tiRowDataStream =
          tiRowDataStream
              .keyBy(
                  new KeySelector<Row, List<Object>>() {
                    @Override
                    public List<Object> getKey(Row row) throws Exception {
                      return columnIndexes.stream()
                          .map(i -> row.get(i, null))
                          .collect(Collectors.toList());
                    }
                  })
              .process(
                  TiDBKeyedProcessFunctionFactory.createKeyedProcessFunction(
                      sinkOptions, tiRowDataStream, uniqueIndexColumnNames));
    }
    return tiRowDataStream;
  }

  private DataStreamSink<?> doConsumeDataStream(
      DataStream<RowData> dataStream, ClientSession clientSession) {
    final byte[] primaryKey;
    final TiTimestamp timestamp = clientSession.getSnapshotVersion();
    final SinkTransaction sinkTransaction = sinkOptions.getSinkTransaction();
    TiTableInfo tiTableInfo = clientSession.getTableMust(databaseName, tableName);

    // check if tidbColumns match flinkColumns
    String[] tidbColumns =
        tiTableInfo.getColumns().stream().map(TiColumnInfo::getName).toArray(String[]::new);
    String[] flinkColumns = tableSchema.getFieldNames();
    Preconditions.checkArgument(
        Arrays.equals(tidbColumns, flinkColumns),
        String.format(
            "Columns do not match:\n " + "tidb -> flink: \n%s",
            SqlUtils.printColumnMapping(tidbColumns, flinkColumns)));

    if (sinkOptions.isDeleteEnable()) {
      // check MINIBATCH
      Preconditions.checkArgument(
          sinkOptions.getSinkTransaction() == MINIBATCH, "delete is only supported in MINIBATCH");
      // check upsert
      Preconditions.checkArgument(
          sinkOptions.getWriteMode() == TiDBWriteMode.UPSERT,
          "delete is only supported in upsert mode");
    } else {
      // filter delete RowKind if delete is disable, delete only work in MINIBATCH with upsert mode
      LOG.info("Flink delete is disabled");
      dataStream = dataStream.filter(row -> row.getRowKind() != RowKind.DELETE);
    }

    // add RowConvertMapFunction
    TiDBRowConverter tiDBRowConverter = new TiDBRowConverter(tiTableInfo);
    DataStream<Row> tiRowDataStream =
        dataStream.map(
            new RowConvertMapFunction(tiDBRowConverter, sinkOptions.isIgnoreAutoincrementColumn()));

    if (sinkTransaction == MINIBATCH) {
      LOG.info("Flink sinkTransaction is working on mode miniBatch");

      // mini batch use row buffer deduplicate
      TiDBWriteOperator tiDBWriteOperator =
          new TiDBMiniBatchWriteOperator(databaseName, tableName, properties, sinkOptions);
      SingleOutputStreamOperator<Void> transform =
          tiRowDataStream.transform("PRE_WRITE", Types.VOID, tiDBWriteOperator);

      // Since sink happens in operator, all we need is to discard stream.
      return transform
          .addSink(new DiscardingSink<>())
          .setParallelism(1)
          .name(DiscardingSink.class.getSimpleName());

    } else if (sinkTransaction == CHECKPOINT) {
      throw new IllegalStateException("CHECKPOINT is not supported yet, please use MINIBATCH");

    } else if (sinkTransaction == GLOBAL) {
      LOG.info("Flink sinkTransaction is working on mode global");

      if (!context.isBounded()) {
        throw new IllegalStateException(
            "Global transaction is invalid for streaming mode or unbounded stream");
      }

      tiRowDataStream = deduplicate(tiRowDataStream, tiTableInfo);

      // preWrite primary key, use random uuid as primary key.
      TiDBWriteHelper tiDBWriteHelper =
          new TiDBWriteHelper(clientSession.getTiSession(), timestamp.getVersion());
      tiDBWriteHelper.preWriteFirst(new BytePairWrapper(fakePrimaryKey(), new byte[0]));
      primaryKey = tiDBWriteHelper.getPrimaryKeyMust();
      tiDBWriteHelper.close();

      // add operator which preWrite secondary keys.
      TiDBWriteOperator tiDBWriteOperator =
          new TiDBGlobalWriteOperator(
              databaseName, tableName, properties, timestamp, sinkOptions, primaryKey);
      SingleOutputStreamOperator<Void> transform =
          tiRowDataStream.transform("PRE_WRITE", Types.VOID, tiDBWriteOperator);

      // add operator which commit primary keys.
      TiDBCommitOperator tiDBCommitOperator =
          new TiDBCommitOperator(properties, timestamp.getVersion(), primaryKey);
      transform = transform.transform("COMMIT", Types.VOID, tiDBCommitOperator).setParallelism(1);

      /*
       Since it's hard to get secondary keys after committing primary keys, we can't
       explicitly commit secondary keys. Add according to Percolator, latter requests
       can resolve locks on secondary keys.
      */
      return transform
          .addSink(new DiscardingSink<>())
          .setParallelism(1)
          .name(DiscardingSink.class.getSimpleName());

    } else {
      throw new IllegalStateException(
          String.format("Unknown transaction mode %s", sinkTransaction));
    }
  }

  private DataStreamSink<Row> generateCheckpointStream(
      DataStream<RowData> dataStream, TiTableInfo tiTableInfo, DataStream<Row> tiRowDataStream) {
    LOG.info("Flink sinkTransaction is working on mode checkpoint");

    // validate if CheckpointingEnabled.
    CheckpointConfig checkpointConfig = dataStream.getExecutionEnvironment().getCheckpointConfig();
    if (!checkpointConfig.isCheckpointingEnabled()) {
      throw new IllegalStateException(
          "Checkpoint transaction is invalid for stream without checkpoint");
    }

    tiRowDataStream = deduplicate(tiRowDataStream, tiTableInfo);

    TiDBSinkFunction sinkFunction =
        new TiDBSinkFunction(
            new TiDBTransactionStateSerializer(),
            new TiDBTransactionContextSerializer(),
            databaseName,
            tableName,
            properties,
            sinkOptions);
    return tiRowDataStream.addSink(sinkFunction);
  }

  @Override
  public DataStreamSink<?> consumeDataStream(DataStream<RowData> dataStream) {
    try (ClientSession clientSession = ClientSession.create(new ClientConfig(properties))) {
      return doConsumeDataStream(dataStream, clientSession);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public static class RowConvertMapFunction implements MapFunction<RowData, Row> {

    private final TiDBRowConverter tiDBRowConverter;
    private final boolean ignoreAutoincrementColumn;

    public RowConvertMapFunction(
        TiDBRowConverter tiDBRowConverter, boolean ignoreAutoincrementColumn) {
      this.tiDBRowConverter = tiDBRowConverter;
      this.ignoreAutoincrementColumn = ignoreAutoincrementColumn;
    }

    @Override
    public Row map(RowData rowData) throws Exception {
      return tiDBRowConverter.toTiRow(rowData, ignoreAutoincrementColumn);
    }
  }
}
