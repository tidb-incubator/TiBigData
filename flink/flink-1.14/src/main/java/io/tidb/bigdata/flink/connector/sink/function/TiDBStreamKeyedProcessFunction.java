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

package io.tidb.bigdata.flink.connector.sink.function;

import com.google.common.base.Objects;
import io.tidb.bigdata.flink.connector.sink.TiDBSinkOptions;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.tikv.common.row.Row;

public class TiDBStreamKeyedProcessFunction extends
    KeyedProcessFunction<List<Object>, Row, Row> implements
    CheckpointListener {

  private final CheckpointConfig checkpointConfig;
  private final TiDBSinkOptions sinkOptions;
  private final List<String> uniqueIndexColumnNames;

  private ValueState<CheckpointEntry> checkpointEntryValueState;
  private CheckpointEntry checkpointEntry;

  public TiDBStreamKeyedProcessFunction(CheckpointConfig checkpointConfig,
      TiDBSinkOptions sinkOptions, List<String> uniqueIndexColumnNames) {
    this.checkpointConfig = checkpointConfig;
    this.sinkOptions = sinkOptions;
    this.uniqueIndexColumnNames = uniqueIndexColumnNames;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.checkpointEntry = new CheckpointEntry(-1 * System.currentTimeMillis(), 0);
    long checkpointInterval = checkpointConfig.getCheckpointInterval();
    long checkpointTimeout = checkpointConfig.getCheckpointTimeout();
    StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(
            Time.milliseconds(2 * (checkpointInterval + checkpointTimeout)))
        .neverReturnExpired()
        .updateTtlOnCreateAndWrite()
        .cleanupInRocksdbCompactFilter(10000)
        .build();

    ValueStateDescriptor<CheckpointEntry> valueStateDescriptor = new ValueStateDescriptor<>(
        "checkpoint-entry-state" + String.join("-", uniqueIndexColumnNames), CheckpointEntry.class);
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
    } else if (!sinkOptions.isDeduplicate()) {
      String names = String.join(", ", uniqueIndexColumnNames);
      String values = ctx.getCurrentKey().stream().map(Object::toString)
          .collect(Collectors.joining(", "));
      throw new IllegalStateException(String.format(
          "Duplicate index in one batch, please enable deduplicate, index: [%s] = [%s]", names,
          values));
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
