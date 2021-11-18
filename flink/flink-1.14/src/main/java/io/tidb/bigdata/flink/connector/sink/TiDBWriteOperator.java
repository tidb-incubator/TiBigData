package io.tidb.bigdata.flink.connector.sink;

import static io.tidb.bigdata.flink.connector.TiDBOptions.SinkTransaction.GLOBAL;

import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.DynamicRowIDAllocator;
import io.tidb.bigdata.tidb.RowBuffer;
import io.tidb.bigdata.tidb.TiDBEncodeHelper;
import io.tidb.bigdata.tidb.TiDBWriteHelper;
import io.tidb.bigdata.tidb.TiDBWriteMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.BytePairWrapper;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.row.Row;

public class TiDBWriteOperator extends AbstractStreamOperator<Void> implements
    OneInputStreamOperator<Row, Void>, BoundedOneInput {

  private static final Logger LOG = LoggerFactory.getLogger(TiDBWriteOperator.class);

  private final String databaseName;
  private final String tableName;
  private final Map<String, String> properties;
  private final TiTimestamp tiTimestamp;
  private final TiDBSinkOptions sinkOptions;
  private final List<Long> rowIdStarts;

  private byte[] primaryKey;

  private transient ClientSession session;
  private transient TiDBEncodeHelper tiDBEncodeHelper;
  private transient TiDBWriteHelper tiDBWriteHelper;
  private transient TiTableInfo tiTableInfo;
  private transient RowBuffer buffer;
  private transient DynamicRowIDAllocator rowIDAllocator;

  public TiDBWriteOperator(String databaseName, String tableName,
      Map<String, String> properties, TiTimestamp tiTimestamp, TiDBSinkOptions sinkOption,
      byte[] primaryKey, List<Long> rowIdStarts) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.properties = properties;
    this.tiTimestamp = tiTimestamp;
    this.sinkOptions = sinkOption;
    this.primaryKey = primaryKey;
    this.rowIdStarts = rowIdStarts;
    if (sinkOption.getSinkTransaction() == GLOBAL && primaryKey == null) {
      throw new IllegalStateException("Primary key can not be null in global transaction");
    }
  }

  @Override
  public void open() throws Exception {
    int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
    Long start = rowIdStarts.get(indexOfThisSubtask);
    this.session = ClientSession.createWithSingleConnection(new ClientConfig(properties));
    this.tiTableInfo = session.getTableMust(databaseName, tableName);
    this.rowIDAllocator = new DynamicRowIDAllocator(session, databaseName, tableName,
        sinkOptions.getRowIdAllocatorStep(), start);
    if (sinkOptions.getSinkTransaction() == GLOBAL) {
      this.buffer = RowBuffer.createDefault(sinkOptions.getBufferSize());
      this.tiDBWriteHelper = new TiDBWriteHelper(session.getTiSession(), tiTimestamp.getVersion(),
          primaryKey);
      this.tiDBEncodeHelper = new TiDBEncodeHelper(
          session,
          tiTimestamp,
          databaseName,
          tableName,
          sinkOptions.isIgnoreAutoincrementColumn(),
          sinkOptions.getWriteMode() == TiDBWriteMode.UPSERT,
          rowIDAllocator);
    } else {
      this.buffer = RowBuffer.createDeduplicateRowBuffer(tiTableInfo,
          sinkOptions.isIgnoreAutoincrementColumn(), sinkOptions.getBufferSize());
    }
  }

  @Override
  public void close() throws Exception {
    if (session != null) {
      session.close();
    }
    Optional.ofNullable(tiDBWriteHelper).ifPresent(TiDBWriteHelper::close);
    Optional.ofNullable(tiDBEncodeHelper).ifPresent(TiDBEncodeHelper::close);
  }

  private void flushRows() {
    if (buffer.size() == 0) {
      return;
    }
    List<BytePairWrapper> pairs = new ArrayList<>(buffer.size());
    if (sinkOptions.getSinkTransaction() == GLOBAL) {
      buffer.getRows().forEach(row -> pairs.addAll(tiDBEncodeHelper.generateKeyValuesByRow(row)));
      tiDBWriteHelper.preWriteSecondKeys(pairs);
    } else {
      // start a new transaction
      TiTimestamp timestamp = session.getSnapshotVersion();
      TiDBEncodeHelper tiDBEncodeHelper = new TiDBEncodeHelper(
          session,
          timestamp,
          databaseName,
          tableName,
          sinkOptions.isIgnoreAutoincrementColumn(),
          sinkOptions.getWriteMode() == TiDBWriteMode.UPSERT,
          rowIDAllocator);
      TiDBWriteHelper tiDBWriteHelper = new TiDBWriteHelper(session.getTiSession(),
          timestamp.getVersion());
      buffer.getRows().forEach(row -> pairs.addAll(tiDBEncodeHelper.generateKeyValuesByRow(row)));
      tiDBWriteHelper.preWriteFirst(pairs);
      tiDBWriteHelper.commitPrimaryKey();
      tiDBWriteHelper.close();
      tiDBEncodeHelper.close();
    }
    buffer.clear();
  }

  @Override
  public void processElement(StreamRecord<Row> element) throws Exception {
    Row row = element.getValue();
    if (buffer.isFull()) {
      flushRows();
    }
    boolean added = buffer.add(row);
    if (!added && !sinkOptions.isDeduplicate()) {
      throw new IllegalStateException(
          "Duplicate index in one batch, please enable deduplicate, row = " + row);
    }
  }

  @Override
  public void endInput() throws Exception {
    if (buffer.size() != 0) {
      flushRows();
    }
  }


}
