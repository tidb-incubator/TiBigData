package io.tidb.bigdata.flink.connector.sink;

import static io.tidb.bigdata.flink.connector.table.TiDBDynamicTableFactory.SinkTransaction.GLOBAL;

import io.tidb.bigdata.flink.connector.table.TiDBDynamicTableFactory.SinkTransaction;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.SqlUtils;
import io.tidb.bigdata.tidb.TiDBWriteHelper;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink.Context;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.BytePairWrapper;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiIndexColumn;
import org.tikv.common.meta.TiIndexInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.row.Row;

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

  public TiDBDataStreamSinkProvider(String databaseName, String tableName,
      ResolvedCatalogTable table, Context context, TiDBSinkOptions sinkOptions) {
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
    List<TiIndexInfo> uniqueIndexes = SqlUtils.getUniqueIndexes(tiTableInfo,
        sinkOptions.isIgnoreAutoincrementColumn());
    if (uniqueIndexes.size() == 0) {
      return tiRowDataStream;
    }
    for (TiIndexInfo uniqueIndex : uniqueIndexes) {
      List<Integer> columnIndexes = uniqueIndex.getIndexColumns()
          .stream()
          .map(TiIndexColumn::getOffset)
          .collect(Collectors.toList());
      tiRowDataStream = tiRowDataStream.keyBy(new KeySelector<Row, List<Object>>() {
        @Override
        public List<Object> getKey(Row row) throws Exception {
          return columnIndexes.stream()
              .map(i -> row.get(i, null))
              .collect(Collectors.toList());
        }
      }).process(TiDBKeyedProcessFunctionFactory.createKeyedProcessFunction(sinkOptions));
    }
    return tiRowDataStream;
  }

  private List<Long> getRowIdStarts(ClientSession session, int parallelism) {
    // create row id start list
    int step = sinkOptions.getRowIdAllocatorStep();
    long start = session.createRowIdAllocator(databaseName, tableName, step * parallelism,
        3).getStart();
    List<Long> rowIdStarts = LongStream.range(0, parallelism)
        .boxed()
        .map(i -> start + i * step)
        .collect(Collectors.toList());
    LOG.info("Create row id starts success, rowIdStarts=" + rowIdStarts);
    return rowIdStarts;

  }

  private DataStreamSink<?> consumeDataStream(DataStream<RowData> dataStream,
      ClientSession session) {
    final byte[] primaryKey;
    int parallelism = dataStream.getParallelism();
    final TiTimestamp timestamp = session.getTimestamp();
    TiTableInfo tiTableInfo = session.getTableMust(databaseName, tableName);
    TiDBRowConverter tiDBRowConverter = new TiDBRowConverter(tiTableInfo,
        sinkOptions.isIgnoreAutoincrementColumn());
    DataStream<Row> tiRowDataStream = dataStream.map(tiDBRowConverter::toTiRow);
    tiRowDataStream = deduplicate(tiRowDataStream, tiTableInfo);
    if (!context.isBounded() && sinkOptions.isUnboundedSourceUseCheckPoint()) {
      if (sinkOptions.getSinkTransaction() == SinkTransaction.GLOBAL) {
        throw new IllegalStateException("Global transaction is invalid for unbounded stream");
      }
      LOG.info("use streaming sink");
      TiDBSinkFunction sinkFunction = new TiDBSinkFunction(
          new TiDBTransactionStateSerializer(),
          new TiDBTransactionContextSerializer(),
          databaseName,
          tableName,
          properties,
          sinkOptions,
          getRowIdStarts(session, parallelism));
      return tiRowDataStream.addSink(sinkFunction);
    }
    String[] tidbColumns = tiTableInfo.getColumns().stream().map(TiColumnInfo::getName)
        .toArray(String[]::new);
    String[] flinkColumns = tableSchema.getFieldNames();
    Preconditions.checkArgument(Arrays.equals(tidbColumns, flinkColumns),
        String.format("Columns do not match:\n "
            + "tidb -> flink: \n%s", SqlUtils.printColumnMapping(tidbColumns, flinkColumns)));
    if (sinkOptions.getSinkTransaction() == GLOBAL) {
      TiDBWriteHelper tiDBWriteHelper = new TiDBWriteHelper(session.getTiSession(),
          timestamp.getVersion());
      // use random uuid as primary key
      tiDBWriteHelper.preWriteFirst(new BytePairWrapper(fakePrimaryKey(), new byte[0]));
      primaryKey = tiDBWriteHelper.getPrimaryKeyMust();
      tiDBWriteHelper.close();
    } else {
      primaryKey = null;
    }
    LOG.info("use batch sink");
    TiDBWriteOperator tiDBWriteOperator = new TiDBWriteOperator(
        databaseName,
        tableName,
        properties,
        timestamp,
        sinkOptions,
        primaryKey,
        getRowIdStarts(session, parallelism));
    SingleOutputStreamOperator<Void> transform = tiRowDataStream.transform("PRE_WRITE",
        Types.VOID,
        tiDBWriteOperator);
    if (sinkOptions.getSinkTransaction() == GLOBAL) {
      TiDBCommitOperator tiDBCommitOperator = new TiDBCommitOperator(properties,
          timestamp.getVersion(), primaryKey);
      transform = transform
          .transform("COMMIT", Types.VOID, tiDBCommitOperator)
          .setParallelism(1);
    }
    return transform
        .addSink(new DiscardingSink<>())
        .setParallelism(1)
        .name(DiscardingSink.class.getSimpleName());
  }

  @Override
  public DataStreamSink<?> consumeDataStream(DataStream<RowData> dataStream) {
    try (ClientSession session = ClientSession.createWithSingleConnection(
        new ClientConfig(properties))) {
      return consumeDataStream(dataStream, session);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

}
