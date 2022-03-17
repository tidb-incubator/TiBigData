package io.tidb.bigdata.flink.connector.sink.operator;

import io.tidb.bigdata.flink.connector.sink.TiDBSinkOptions;
import io.tidb.bigdata.tidb.RowBuffer;
import io.tidb.bigdata.tidb.TiDBEncodeHelper;
import io.tidb.bigdata.tidb.TiDBWriteHelper;
import io.tidb.bigdata.tidb.TiDBWriteMode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.tikv.common.BytePairWrapper;
import org.tikv.common.ByteWrapper;
import org.tikv.common.meta.TiTimestamp;

/**
 * The operator for flushing rows to TiDB on mode MiniBatch.
 * During each flush, the rows are committed to TiDB in a transaction.
 */
public class TiDBMiniBatchWriteOperator extends TiDBWriteOperator {

  public TiDBMiniBatchWriteOperator(String databaseName, String tableName,
      Map<String, String> properties, TiTimestamp tiTimestamp,
      TiDBSinkOptions sinkOption, List<Long> rowIdStarts) {
    super(databaseName, tableName, properties, tiTimestamp, sinkOption, null, rowIdStarts);
  }

  @Override
  protected void openInternal() {
    this.buffer = RowBuffer.createDeduplicateRowBuffer(tiTableInfo,
        sinkOptions.isIgnoreAutoincrementColumn(), sinkOptions.getBufferSize());
  }

  @Override
  protected void flushRows() {
    if (buffer.size() == 0) {
      return;
    }
    List<BytePairWrapper> pairs = new ArrayList<>(buffer.size());

    // start a new transaction
    TiTimestamp timestamp = session.getSnapshotVersion();
    tiDBEncodeHelper = new TiDBEncodeHelper(
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
    long commitTs = tiDBWriteHelper.commitPrimaryKey();

    Iterator<ByteWrapper> secondaryKeys = pairs.stream()
        .map(bytePairWrapper -> new ByteWrapper(bytePairWrapper.getKey()))
        .collect(Collectors.toList()).iterator();
    secondaryKeys.next();

    tiDBWriteHelper.commitSecondaryKeys(secondaryKeys, commitTs);
    tiDBWriteHelper.close();
    tiDBEncodeHelper.close();

    buffer.clear();
  }

  /**
   * We need to flush the buffer before checkpointing in case of data loss.
   */
  @Override
  public void snapshotState(StateSnapshotContext context) {
    flushRows();
  }
}