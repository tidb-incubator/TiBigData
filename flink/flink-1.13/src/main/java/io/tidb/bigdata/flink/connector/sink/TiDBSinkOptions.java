package io.tidb.bigdata.flink.connector.sink;

import static io.tidb.bigdata.flink.connector.table.TiDBDynamicTableFactory.IGNORE_AUTOINCREMENT_COLUMN_VALUE;
import static io.tidb.bigdata.flink.connector.table.TiDBDynamicTableFactory.ROW_ID_ALLOCATOR_STEP;
import static io.tidb.bigdata.flink.connector.table.TiDBDynamicTableFactory.SINK_BUFFER_SIZE;
import static io.tidb.bigdata.flink.connector.table.TiDBDynamicTableFactory.SINK_IMPL;
import static io.tidb.bigdata.flink.connector.table.TiDBDynamicTableFactory.SINK_TRANSACTION;
import static io.tidb.bigdata.flink.connector.table.TiDBDynamicTableFactory.UNBOUNDED_SOURCE_USE_CHECKPOINT_SINK;

import io.tidb.bigdata.flink.connector.table.TiDBDynamicTableFactory.SinkImpl;
import io.tidb.bigdata.flink.connector.table.TiDBDynamicTableFactory.SinkTransaction;
import java.io.Serializable;
import org.apache.flink.configuration.ReadableConfig;

public class TiDBSinkOptions implements Serializable {

  private final SinkImpl sinkImpl;
  private final SinkTransaction sinkTransaction;
  private final int bufferSize;
  private final int rowIdAllocatorStep;
  private final boolean unboundedSourceUseCheckPoint;
  private final boolean ignoreAutoincrementColumn;

  public TiDBSinkOptions(ReadableConfig config) {
    this.sinkImpl = config.get(SINK_IMPL);
    this.sinkTransaction = config.get(SINK_TRANSACTION);
    this.bufferSize = config.get(SINK_BUFFER_SIZE);
    this.rowIdAllocatorStep = config.get(ROW_ID_ALLOCATOR_STEP);
    if (rowIdAllocatorStep < 1000 || rowIdAllocatorStep > 1000000) {
      throw new IllegalArgumentException(
          "tidb.sink.row-id-allocator.step must in range [1000,1000000]");
    }
    this.unboundedSourceUseCheckPoint = config.get(UNBOUNDED_SOURCE_USE_CHECKPOINT_SINK);
    this.ignoreAutoincrementColumn = config.get(IGNORE_AUTOINCREMENT_COLUMN_VALUE);
  }

  public SinkImpl getSinkImpl() {
    return sinkImpl;
  }

  public SinkTransaction getSinkTransaction() {
    return sinkTransaction;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public int getRowIdAllocatorStep() {
    return rowIdAllocatorStep;
  }

  public boolean isUnboundedSourceUseCheckPoint() {
    return unboundedSourceUseCheckPoint;
  }

  public boolean isIgnoreAutoincrementColumn() {
    return ignoreAutoincrementColumn;
  }
}
