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

import io.tidb.bigdata.flink.connector.sink.TiDBSinkOptions;
import io.tidb.bigdata.flink.connector.sink.function.TiDBSinkFunction.TiDBTransactionContext;
import io.tidb.bigdata.flink.connector.sink.function.TiDBSinkFunction.TiDBTransactionState;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.TiDBWriteHelper;
import io.tidb.bigdata.tidb.TiDBWriteMode;
import io.tidb.bigdata.tidb.allocator.DynamicRowIDAllocator;
import io.tidb.bigdata.tidb.codec.TiDBEncodeHelper;
import io.tidb.bigdata.tidb.row.Row;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.BytePairWrapper;
import org.tikv.common.meta.TiTimestamp;

/**
 * ATTENTIONS: Right now, this class has not provided exactly-once semantic.
 *
 * <p>A sink function that sinks unbounded stream with exactly-once semantic.
 *
 * <p><b>NOTE:</b> There is still a potential of data loss. Please See {@link
 * TwoPhaseCommitSinkFunction#recoverAndCommit(Object)}
 */
@Deprecated
public class TiDBSinkFunction
    extends TwoPhaseCommitSinkFunction<Row, TiDBTransactionState, TiDBTransactionContext> {

  private static final Logger LOG = LoggerFactory.getLogger(TiDBSinkFunction.class);

  private final String databaseName;
  private final String tableName;
  private final Map<String, String> properties;
  private final TiDBSinkOptions sinkOptions;
  private final Map<String, TiDBWriteHelper> tiDBWriteHelpers;
  private final Map<String, TiDBEncodeHelper> tiDBEncodeHelpers;
  private final Map<String, List<Row>> buffers;

  private boolean init;

  private transient ClientSession session;

  public TiDBSinkFunction(
      TypeSerializer<TiDBTransactionState> transactionSerializer,
      TypeSerializer<TiDBTransactionContext> contextSerializer,
      String databaseName,
      String tableName,
      Map<String, String> properties,
      TiDBSinkOptions sinkOptions) {
    super(transactionSerializer, contextSerializer);
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.properties = properties;
    this.sinkOptions = sinkOptions;
    this.tiDBWriteHelpers = new HashMap<>();
    this.tiDBEncodeHelpers = new HashMap<>();
    this.buffers = new HashMap<>();
  }

  private void makeSureInit() {
    if (init) {
      return;
    }
    this.session = ClientSession.create(new ClientConfig(properties));
    this.init = true;
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (session != null) {
      session.close();
    }

    tiDBWriteHelpers.values().forEach(TiDBWriteHelper::close);
    tiDBEncodeHelpers.values().forEach(TiDBEncodeHelper::close);

    tiDBEncodeHelpers.clear();
    tiDBWriteHelpers.clear();

    LOG.info("TiDBSinkFunction has been closed");
  }

  private void flushRows(TiDBTransactionState tiDBTransactionState) {
    String transactionId = tiDBTransactionState.getTransactionId();
    List<Row> buffer = buffers.get(transactionId);
    if (buffer.size() == 0) {
      return;
    }
    List<BytePairWrapper> pairs = new ArrayList<>(buffer.size());
    TiDBEncodeHelper tiDBEncodeHelper = tiDBEncodeHelpers.get(transactionId);
    TiDBWriteHelper tiDBWriteHelper = tiDBWriteHelpers.get(transactionId);
    buffer.forEach(row -> pairs.addAll(tiDBEncodeHelper.generateKeyValuesByRow(row)));
    if (tiDBWriteHelper.getPrimaryKey().isPresent()) {
      tiDBWriteHelper.preWriteSecondKeys(pairs);
    } else {
      tiDBWriteHelper.preWriteFirst(pairs);
    }
    buffers.put(transactionId, new ArrayList<>(sinkOptions.getBufferSize()));
  }

  @Override
  protected void invoke(TiDBTransactionState transaction, Row row, Context context)
      throws Exception {
    String transactionId = transaction.getTransactionId();
    List<Row> buffer = this.buffers.get(transactionId);
    if (buffer.size() >= sinkOptions.getBufferSize()) {
      flushRows(transaction);
    }
    buffers.get(transactionId).add(row);
  }

  @Override
  protected TiDBTransactionState beginTransaction() throws Exception {
    makeSureInit();
    String transactionId = UUID.randomUUID().toString();
    buffers.put(transactionId, new ArrayList<>(sinkOptions.getBufferSize()));
    TiTimestamp timestamp = session.getSnapshotVersion();
    TiDBWriteHelper tiDBWriteHelper =
        new TiDBWriteHelper(session.getTiSession(), timestamp.getVersion());
    DynamicRowIDAllocator rowIDAllocator =
        new DynamicRowIDAllocator(
            session, databaseName, tableName, sinkOptions.getRowIdAllocatorStep(), null, timestamp);
    TiDBEncodeHelper tiDBEncodeHelper =
        new TiDBEncodeHelper(
            session,
            timestamp,
            databaseName,
            tableName,
            sinkOptions.isIgnoreAutoincrementColumn(),
            sinkOptions.isIgnoreAutoRandomColumn(),
            sinkOptions.getWriteMode() == TiDBWriteMode.UPSERT,
            rowIDAllocator);
    tiDBWriteHelpers.put(transactionId, tiDBWriteHelper);
    tiDBEncodeHelpers.put(transactionId, tiDBEncodeHelper);
    return new TiDBTransactionState(transactionId, timestamp.getPhysical(), timestamp.getLogical());
  }

  @Override
  protected void preCommit(TiDBTransactionState transaction) throws Exception {
    flushRows(transaction);
  }

  @Override
  protected void commit(TiDBTransactionState transaction) {
    String transactionId = transaction.getTransactionId();
    Optional.ofNullable(tiDBWriteHelpers.remove(transactionId))
        .ifPresent(
            tiDBWriteHelper -> {
              tiDBWriteHelper
                  .getPrimaryKey()
                  .ifPresent(primaryKey -> tiDBWriteHelper.commitPrimaryKey());
              tiDBWriteHelper.close();
            });
    Optional.ofNullable(tiDBEncodeHelpers.remove(transactionId)).ifPresent(TiDBEncodeHelper::close);
  }

  @Override
  protected void abort(TiDBTransactionState transaction) {
    String transactionId = transaction.getTransactionId();
    Optional.ofNullable(tiDBWriteHelpers.remove(transactionId)).ifPresent(TiDBWriteHelper::close);
    Optional.ofNullable(tiDBEncodeHelpers.remove(transactionId)).ifPresent(TiDBEncodeHelper::close);
    buffers.remove(transactionId);
  }

  public static class TiDBTransactionState {

    private final String transactionId;
    private final long physicalTs;
    private final long logicalTs;

    public TiDBTransactionState(String transactionId, long physicalTs, long logicalTs) {
      this.transactionId = transactionId;
      this.physicalTs = physicalTs;
      this.logicalTs = logicalTs;
    }

    public String getTransactionId() {
      return transactionId;
    }

    public long getPhysicalTs() {
      return physicalTs;
    }

    public long getLogicalTs() {
      return logicalTs;
    }
  }

  public static class TiDBTransactionContext {}
}
