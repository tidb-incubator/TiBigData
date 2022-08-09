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

package io.tidb.bigdata.flink.connector.sink.operator;

import io.tidb.bigdata.flink.connector.sink.TiDBSinkOptions;
import io.tidb.bigdata.flink.connector.utils.TiRow;
import io.tidb.bigdata.tidb.TiDBWriteHelper;
import io.tidb.bigdata.tidb.TiDBWriteMode;
import io.tidb.bigdata.tidb.allocator.DynamicRowIDAllocator;
import io.tidb.bigdata.tidb.buffer.RowBuffer;
import io.tidb.bigdata.tidb.codec.TiDBEncodeHelper;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.types.RowKind;
import org.tikv.common.BytePairWrapper;
import org.tikv.common.ByteWrapper;
import org.tikv.common.meta.TiTimestamp;

/**
 * The operator for flushing rows to TiDB on mode MiniBatch. During each flush, the rows are
 * committed to TiDB in a transaction.
 */
public class TiDBMiniBatchWriteOperator extends TiDBWriteOperator {

  public TiDBMiniBatchWriteOperator(
      String databaseName,
      String tableName,
      Map<String, String> properties,
      TiDBSinkOptions sinkOption) {
    super(databaseName, tableName, properties, sinkOption, null);
  }

  @Override
  protected void openInternal() {
    this.buffer =
        RowBuffer.createDeduplicateRowBuffer(
            tiTableInfo, sinkOptions.isIgnoreAutoincrementColumn(), sinkOptions.getBufferSize());
  }

  @Override
  protected void flushRows() {
    if (buffer.size() == 0) {
      return;
    }
    List<BytePairWrapper> pairs = new ArrayList<>(buffer.size());

    // start a new transaction
    TiTimestamp timestamp = session.getSnapshotVersion();
    DynamicRowIDAllocator rowIDAllocator =
        new DynamicRowIDAllocator(
            session, databaseName, tableName, sinkOptions.getRowIdAllocatorStep(), timestamp);
    tiDBEncodeHelper =
        new TiDBEncodeHelper(
            session,
            timestamp,
            databaseName,
            tableName,
            sinkOptions.isIgnoreAutoincrementColumn(),
            sinkOptions.isIgnoreAutoRandomColumn(),
            sinkOptions.getWriteMode() == TiDBWriteMode.UPSERT,
            rowIDAllocator);
    TiDBWriteHelper tiDBWriteHelper =
        new TiDBWriteHelper(session.getTiSession(), timestamp.getVersion());

    // insert or upsert
    buffer.getRows().stream()
        .filter(row -> ((TiRow) row).getRowKind() != RowKind.DELETE)
        .forEach(row -> pairs.addAll(tiDBEncodeHelper.generateKeyValuesByRow(row)));
    // delete
    buffer.getRows().stream()
        .filter(row -> ((TiRow) row).getRowKind() == RowKind.DELETE)
        .forEach(row -> pairs.addAll(tiDBEncodeHelper.generateKeyValuesToDeleteByRow(row)));

    if (!pairs.isEmpty()) {
      tiDBWriteHelper.preWriteFirst(pairs);
      long commitTs = tiDBWriteHelper.commitPrimaryKey();

      Iterator<ByteWrapper> secondaryKeys =
          pairs.stream()
              .map(bytePairWrapper -> new ByteWrapper(bytePairWrapper.getKey()))
              .collect(Collectors.toList())
              .iterator();
      secondaryKeys.next();

      tiDBWriteHelper.commitSecondaryKeys(secondaryKeys, commitTs);
    }

    tiDBWriteHelper.close();
    tiDBEncodeHelper.close();

    buffer.clear();
  }

  /** We need to flush the buffer before checkpointing in case of data loss. */
  @Override
  public void snapshotState(StateSnapshotContext context) {
    flushRows();
  }
}
