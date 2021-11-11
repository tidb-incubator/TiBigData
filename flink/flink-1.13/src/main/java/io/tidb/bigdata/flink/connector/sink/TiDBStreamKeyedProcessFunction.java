package io.tidb.bigdata.flink.connector.sink;

import com.google.common.base.Objects;
import java.io.Serializable;
import java.util.List;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.tikv.common.row.Row;

public class TiDBStreamKeyedProcessFunction extends
    KeyedProcessFunction<List<Object>, Row, Row> implements
    CheckpointListener {

  private ValueState<CheckpointEntry> checkpointEntryValueState;
  private CheckpointEntry checkpointEntry;

  @Override
  public void open(Configuration parameters) throws Exception {
    this.checkpointEntry = new CheckpointEntry(-1 * System.currentTimeMillis(), 0);
    StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1))
        .neverReturnExpired()
        .updateTtlOnCreateAndWrite()
        .cleanupInRocksdbCompactFilter(10000)
        .build();

    ValueStateDescriptor<CheckpointEntry> valueStateDescriptor = new ValueStateDescriptor<>(
        "checkpoint-entry-state", CheckpointEntry.class);
    valueStateDescriptor.enableTimeToLive(stateTtlConfig);
    this.checkpointEntryValueState = this.getRuntimeContext().getState(valueStateDescriptor);
  }

  @Override
  public void processElement(Row row, KeyedProcessFunction<List<Object>, Row, Row>.Context ctx,
      Collector<Row> out) throws Exception {
    CheckpointEntry value = checkpointEntryValueState.value();
    if (value == null || !value.equals(checkpointEntry)) {
      checkpointEntryValueState.update(checkpointEntry.copy());
      out.collect(row);
    }
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    checkpointEntry.setCheckpointId(++checkpointId);
    checkpointEntry.setAbortCount(0);
  }

  @Override
  public void notifyCheckpointAborted(long checkpointId) throws Exception {
    checkpointEntry.addAbortCount();
  }

  public static final class CheckpointEntry implements Serializable {

    private long checkpointId;
    private long abortCount;

    public CheckpointEntry(long checkpointId, long abortCount) {
      this.checkpointId = checkpointId;
      this.abortCount = abortCount;
    }

    public long getCheckpointId() {
      return checkpointId;
    }

    public void setCheckpointId(long checkpointId) {
      this.checkpointId = checkpointId;
    }

    public long getAbortCount() {
      return abortCount;
    }

    public void setAbortCount(long abortCount) {
      this.abortCount = abortCount;
    }

    public void addAbortCount() {
      this.abortCount++;
    }

    public CheckpointEntry copy() {
      return new CheckpointEntry(checkpointId, abortCount);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CheckpointEntry that = (CheckpointEntry) o;
      return checkpointId == that.checkpointId && abortCount == that.abortCount;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(checkpointId, abortCount);
    }

    @Override
    public String toString() {
      return "CheckpointEntry{"
          + "checkpointId=" + checkpointId
          + ", abortCount=" + abortCount
          + '}';
    }
  }
}
