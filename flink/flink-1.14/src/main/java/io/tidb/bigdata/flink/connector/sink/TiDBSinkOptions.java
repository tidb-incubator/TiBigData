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

package io.tidb.bigdata.flink.connector.sink;

import static io.tidb.bigdata.flink.connector.TiDBOptions.DEDUPLICATE;
import static io.tidb.bigdata.flink.connector.TiDBOptions.IGNORE_AUTOINCREMENT_COLUMN_VALUE;
import static io.tidb.bigdata.flink.connector.TiDBOptions.ROW_ID_ALLOCATOR_STEP;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SINK_BUFFER_SIZE;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SINK_IMPL;
import static io.tidb.bigdata.flink.connector.TiDBOptions.SINK_TRANSACTION;
import static io.tidb.bigdata.flink.connector.TiDBOptions.WRITE_MODE;

import io.tidb.bigdata.flink.connector.TiDBOptions.SinkImpl;
import io.tidb.bigdata.flink.connector.TiDBOptions.SinkTransaction;
import io.tidb.bigdata.tidb.TiDBWriteMode;
import java.io.Serializable;
import org.apache.flink.configuration.ReadableConfig;

public class TiDBSinkOptions implements Serializable {

  private final SinkImpl sinkImpl;
  private final SinkTransaction sinkTransaction;
  private final int bufferSize;
  private final int rowIdAllocatorStep;
  private final boolean ignoreAutoincrementColumn;
  private final boolean deduplicate;
  private final TiDBWriteMode writeMode;

  public TiDBSinkOptions(ReadableConfig config) {
    this.sinkImpl = config.get(SINK_IMPL);
    this.sinkTransaction = config.get(SINK_TRANSACTION);
    this.bufferSize = config.get(SINK_BUFFER_SIZE);
    this.rowIdAllocatorStep = config.get(ROW_ID_ALLOCATOR_STEP);
    this.ignoreAutoincrementColumn = config.get(IGNORE_AUTOINCREMENT_COLUMN_VALUE);
    this.deduplicate = config.get(DEDUPLICATE);
    this.writeMode = TiDBWriteMode.fromString(config.get(WRITE_MODE));
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

  public boolean isIgnoreAutoincrementColumn() {
    return ignoreAutoincrementColumn;
  }

  public boolean isDeduplicate() {
    return deduplicate;
  }

  public TiDBWriteMode getWriteMode() {
    return writeMode;
  }
}
