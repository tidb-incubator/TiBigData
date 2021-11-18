package io.tidb.bigdata.flink.connector.sink;

import io.tidb.bigdata.flink.connector.sink.TiDBSinkFunction.TiDBTransactionContext;
import io.tidb.bigdata.flink.connector.sink.TiDBSinkFunction.TiDBTransactionState;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.DynamicRowIDAllocator;
import io.tidb.bigdata.tidb.TiDBEncodeHelper;
import io.tidb.bigdata.tidb.TiDBWriteHelper;
import io.tidb.bigdata.tidb.TiDBWriteMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.BytePairWrapper;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.row.Row;

public class TiDBSinkFunction extends
    TwoPhaseCommitSinkFunction<Row, TiDBTransactionState, TiDBTransactionContext> {

  private static final Logger LOG = LoggerFactory.getLogger(TiDBSinkFunction.class);

  private final String databaseName;
  private final String tableName;
  private final Map<String, String> properties;
  private final TiDBSinkOptions sinkOptions;
  private final Map<String, TiDBWriteHelper> tiDBWriteHelpers;
  private final Map<String, TiDBEncodeHelper> tiDBEncodeHelpers;
  private final Map<String, List<Row>> buffers;

  private boolean init;

  private transient ClientSession session;
  private transient TiTableInfo tiTableInfo;

  private transient DynamicRowIDAllocator rowIDAllocator;


  public TiDBSinkFunction(
      TypeSerializer<TiDBTransactionState> transactionSerializer,
      TypeSerializer<TiDBTransactionContext> contextSerializer,
      String databaseName,
      String tableName,
      Map<String, String> properties,
      TiDBSinkOptions sinkOptions) {
    super(transactionSerializer, contextSerializer);
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.properties = properties;
    this.sinkOptions = sinkOptions;
    this.tiDBWriteHelpers = new HashMap<>();
    this.tiDBEncodeHelpers = new HashMap<>();
    this.buffers = new HashMap<>();
  }

  private void makeSureInit() {
    if (init) {
      return;
    }
    this.session = ClientSession.createWithSingleConnection(new ClientConfig(properties));
    this.tiTableInfo = session.getTiSession().getCatalog().getTable(databaseName, tableName);
    this.init = true;
    this.rowIDAllocator = new DynamicRowIDAllocator(session, databaseName, tableName,
        sinkOptions.getRowIdAllocatorStep(), null);
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (session != null) {
      session.close();
    }
  }

  private void flushRows(TiDBTransactionState tiDBTransactionState) {
    String transactionId = tiDBTransactionState.getTransactionId();
    List<Row> buffer = buffers.get(transactionId);
    if (buffer.size() == 0) {
      return;
    }
    List<BytePairWrapper> pairs = new ArrayList<>(buffer.size());
    TiDBEncodeHelper tiDBEncodeHelper = tiDBEncodeHelpers.get(transactionId);
    TiDBWriteHelper tiDBWriteHelper = tiDBWriteHelpers.get(transactionId);
    buffer.forEach(
        row -> pairs.addAll(tiDBEncodeHelper.generateKeyValuesByRow(row)));
    if (tiDBWriteHelper.getPrimaryKey().isPresent()) {
      tiDBWriteHelper.preWriteSecondKeys(pairs);
    } else {
      tiDBWriteHelper.preWriteFirst(pairs);
    }
    buffers.put(transactionId, new ArrayList<>(sinkOptions.getBufferSize()));
  }

  @Override
  protected void invoke(TiDBTransactionState transaction, Row row, Context context)
      throws Exception {
    String transactionId = transaction.getTransactionId();
    List<Row> buffer = this.buffers.get(transactionId);
    if (buffer.size() >= sinkOptions.getBufferSize()) {
      flushRows(transaction);
    }
    buffers.get(transactionId).add(row);
  }

  @Override
  protected TiDBTransactionState beginTransaction() throws Exception {
    makeSureInit();
    String transactionId = UUID.randomUUID().toString();
    buffers.put(transactionId, new ArrayList<>(sinkOptions.getBufferSize()));
    TiTimestamp timestamp = session.getTimestamp();
    TiDBWriteHelper tiDBWriteHelper = new TiDBWriteHelper(session.getTiSession(),
        timestamp.getVersion());
    TiDBEncodeHelper tiDBEncodeHelper = new TiDBEncodeHelper(
        session,
        timestamp,
        databaseName,
        tableName,
        sinkOptions.isIgnoreAutoincrementColumn(),
        sinkOptions.getWriteMode() == TiDBWriteMode.UPSERT,
        rowIDAllocator);
    tiDBWriteHelpers.put(transactionId, tiDBWriteHelper);
    tiDBEncodeHelpers.put(transactionId, tiDBEncodeHelper);
    return new TiDBTransactionState(transactionId, timestamp.getPhysical(), timestamp.getLogical());
  }

  @Override
  protected void preCommit(TiDBTransactionState transaction) throws Exception {
    flushRows(transaction);
  }

  @Override
  protected void commit(TiDBTransactionState transaction) {
    String transactionId = transaction.getTransactionId();
    Optional.ofNullable(tiDBWriteHelpers.remove(transactionId)).ifPresent(tiDBWriteHelper -> {
      tiDBWriteHelper.getPrimaryKey().ifPresent(primaryKey -> tiDBWriteHelper.commitPrimaryKey());
      tiDBWriteHelper.close();
    });
    Optional.ofNullable(tiDBEncodeHelpers.remove(transactionId)).ifPresent(TiDBEncodeHelper::close);
  }

  @Override
  protected void abort(TiDBTransactionState transaction) {
    String transactionId = transaction.getTransactionId();
    Optional.ofNullable(tiDBWriteHelpers.remove(transactionId)).ifPresent(TiDBWriteHelper::close);
    Optional.ofNullable(tiDBEncodeHelpers.remove(transactionId)).ifPresent(TiDBEncodeHelper::close);
    buffers.remove(transactionId);
  }

  public static class TiDBTransactionState {

    private final String transactionId;
    private final long physicalTs;
    private final long logicalTs;


    public TiDBTransactionState(String transactionId, long physicalTs, long logicalTs) {
      this.transactionId = transactionId;
      this.physicalTs = physicalTs;
      this.logicalTs = logicalTs;
    }

    public String getTransactionId() {
      return transactionId;
    }

    public long getPhysicalTs() {
      return physicalTs;
    }

    public long getLogicalTs() {
      return logicalTs;
    }
  }

  public static class TiDBTransactionContext {

  }


}
