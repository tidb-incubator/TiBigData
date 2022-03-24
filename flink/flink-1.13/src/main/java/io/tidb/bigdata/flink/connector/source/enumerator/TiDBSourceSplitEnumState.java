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
import java.util.List;
import org.tikv.common.meta.TiTimestamp;

public class TiDBSourceSplitEnumState {

  private final List<TiDBSourceSplit> splits;
  // Serialize timestamp to avoid split is empty.
  private final TiTimestamp timestamp;

  public TiDBSourceSplitEnumState(List<TiDBSourceSplit> splits, TiTimestamp timestamp) {
    this.splits = splits;
    this.timestamp = timestamp;
  }

  public List<TiDBSourceSplit> getSplits() {
    return splits;
  }

  public TiTimestamp getTimestamp() {
    return timestamp;
  }
}