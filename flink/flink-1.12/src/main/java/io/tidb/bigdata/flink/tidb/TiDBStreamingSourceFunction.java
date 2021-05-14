package io.tidb.bigdata.flink.tidb;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.source.ScanTableSource.ScanRuntimeProvider;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.ExceptionUtils;

public class TiDBStreamingSourceFunction extends RichSourceFunction<RowData>
    implements CheckpointedFunction, CheckpointListener, ResultTypeQueryable<RowData> {

  private final TiDBRowDataInputFormat inputFormat;
  private final SourceFunction<RowData> sourceFunction;
  private final CheckpointedFunction checkpointedFunction;
  private final CheckpointListener checkpointListener;
  private final AbstractRichFunction abstractRichFunction;
  private final ResultTypeQueryable<RowData> resultTypeQueryable;

  private final AtomicBoolean runningSnapshot = new AtomicBoolean(true);

  public TiDBStreamingSourceFunction(TiDBRowDataInputFormat inputFormat,
      ScanRuntimeProvider streamingProvider) {
    this.inputFormat = inputFormat;
    this.sourceFunction = ((SourceFunctionProvider) streamingProvider).createSourceFunction();
    if (sourceFunction instanceof CheckpointListener) {
      this.checkpointListener = (CheckpointListener) sourceFunction;
    } else {
      this.checkpointListener = null;
    }
    if (sourceFunction instanceof CheckpointedFunction) {
      this.checkpointedFunction = (CheckpointedFunction) sourceFunction;
    } else {
      this.checkpointedFunction = null;
    }
    if (sourceFunction instanceof AbstractRichFunction) {
      this.abstractRichFunction = (AbstractRichFunction) sourceFunction;
    } else {
      this.abstractRichFunction = null;
    }
    if (sourceFunction instanceof ResultTypeQueryable) {
      this.resultTypeQueryable = (ResultTypeQueryable) sourceFunction;
    } else {
      this.resultTypeQueryable = null;
    }
    if (abstractRichFunction == null) {
      return;
    }
  }

  @Override
  public void setRuntimeContext(RuntimeContext t) {
    super.setRuntimeContext(t);
    this.abstractRichFunction.setRuntimeContext(t);
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    if (checkpointListener == null) {
      return;
    }
    checkpointListener.notifyCheckpointComplete(checkpointId);
  }

  @Override
  public void notifyCheckpointAborted(long checkpointId) throws Exception {
    if (checkpointListener == null) {
      return;
    }
    checkpointListener.notifyCheckpointAborted(checkpointId);
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    if (resultTypeQueryable == null) {
      return null;
    }
    return resultTypeQueryable.getProducedType();
  }

  @Override
  public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
    if (checkpointedFunction == null || runningSnapshot.get()) {
      return;
    }
    checkpointedFunction.snapshotState(functionSnapshotContext);
  }

  @Override
  public void initializeState(FunctionInitializationContext functionInitializationContext)
      throws Exception {
    if (checkpointedFunction == null) {
      return;
    }
    checkpointedFunction.initializeState(functionInitializationContext);
  }

  private void runBatch(SourceContext<RowData> sourceContext) throws Exception {
    try {
      inputFormat.openInputFormat();
      for (InputSplit split : inputFormat.createInputSplits(1)) {
        inputFormat.open(split);
        while (!inputFormat.reachedEnd()) {
          sourceContext.collect(inputFormat.nextRecord(null));
        }
        ExceptionUtils.suppressExceptions(() -> inputFormat.close());
      }
    } finally {
      ExceptionUtils.suppressExceptions(() -> inputFormat.closeInputFormat());
      this.runningSnapshot.set(false);
    }
  }

  private void runStreaming(SourceContext<RowData> sourceContext) throws Exception {
    if (sourceFunction == null) {
      return;
    }
    sourceFunction.run(sourceContext);
  }

  @Override
  public void run(SourceContext<RowData> sourceContext) throws Exception {
    runBatch(sourceContext);
    runStreaming(sourceContext);
  }

  @Override
  public void cancel() {
    if (sourceFunction == null) {
      return;
    }
    sourceFunction.cancel();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    abstractRichFunction.open(parameters);
  }

  @Override
  public void close() throws Exception {
    if (abstractRichFunction == null) {
      return;
    }
    abstractRichFunction.close();
  }
}