/*
 * Copyright 2021 TiDB Project Authors.
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

package io.tidb.bigdata.flink.connector.source.enumerator;

import io.tidb.bigdata.flink.connector.source.split.TiDBSourceSplit;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.meta.TiTimestamp;

public class TiDBSourceSplitEnumerator implements
    SplitEnumerator<TiDBSourceSplit, TiDBSourceSplitEnumState> {

  private static final Logger LOG = LoggerFactory.getLogger(TiDBSourceSplitEnumerator.class);

  private final Queue<TiDBSourceSplit> remainingSplits;
  private final TiTimestamp timestamp;
  private final SplitEnumeratorContext<TiDBSourceSplit> context;

  public TiDBSourceSplitEnumerator(
      List<TiDBSourceSplit> splits,
      TiTimestamp timestamp,
      SplitEnumeratorContext<TiDBSourceSplit> context) {
    this.remainingSplits = new ArrayDeque<>(splits);
    this.context = context;
    this.timestamp = timestamp;
  }


  @Override
  public void start() {

  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    TiDBSourceSplit nextSplit = remainingSplits.poll();
    if (nextSplit != null) {
      context.assignSplit(nextSplit, subtaskId);
    } else {
      context.signalNoMoreSplits(subtaskId);
    }
  }

  @Override
  public void addSplitsBack(List<TiDBSourceSplit> splits, int subtaskId) {
    remainingSplits.addAll(splits);
  }

  @Override
  public void addReader(int subtaskId) {

  }

  @Override
  public TiDBSourceSplitEnumState snapshotState(long checkpointId) throws Exception {
    return new TiDBSourceSplitEnumState(new ArrayList<>(remainingSplits), timestamp);
  }

  @Override
  public void close() throws IOException {

  }

  public TiTimestamp getTimestamp() {
    return timestamp;
  }
}
