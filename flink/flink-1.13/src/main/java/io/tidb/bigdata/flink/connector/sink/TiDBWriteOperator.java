package io.tidb.bigdata.flink.connector.sink;

import static io.tidb.bigdata.flink.connector.table.TiDBDynamicTableFactory.SinkTransaction.GLOBAL;
import static io.tidb.bigdata.flink.connector.table.TiDBDynamicTableFactory.SinkTransaction.LOCAL;

import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.DynamicRowIDAllocator;
import io.tidb.bigdata.tidb.TiDBEncodeHelper;
import io.tidb.bigdata.tidb.TiDBWriteHelper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.BytePairWrapper;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.row.Row;

public class TiDBWriteOperator extends AbstractStreamOperator<Void> implements
    OneInputStreamOperator<Row, Void>, BoundedOneInput {

  private static final Logger LOG = LoggerFactory.getLogger(TiDBWriteOperator.class);

  private final String databaseName;
  private final String tableName;
  private final Map<String, String> properties;
  private final TiTimestamp tiTimestamp;
  private final TiDBSinkOptions sinkOptions;
  private final List<Long> rowIdStarts;

  private byte[] primaryKey;

  private transient ClientSession session;
  private transient TiDBEncodeHelper tiDBEncodeHelper;
  private transient TiDBWriteHelper tiDBWriteHelper;
  private transient TiTableInfo tiTableInfo;
  private transient List<Row> buffer;
  private transient DynamicRowIDAllocator rowIDAllocator;

  public TiDBWriteOperator(String databaseName, String tableName,
      Map<String, String> properties, TiTimestamp tiTimestamp, TiDBSinkOptions sinkOption,
      byte[] primaryKey, List<Long> rowIdStarts) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.properties = properties;
    this.tiTimestamp = tiTimestamp;
    this.sinkOptions = sinkOption;
    this.primaryKey = primaryKey;
    this.rowIdStarts = rowIdStarts;
    if (sinkOption.getSinkTransaction() == GLOBAL && primaryKey == null) {
      throw new IllegalStateException("Primary key can not be null in global transaction");
    }
  }

  @Override
  public void open() throws Exception {
    int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
    Long start = rowIdStarts.get(indexOfThisSubtask);
    this.session = ClientSession.createWithSingleConnection(new ClientConfig(properties));
    this.rowIDAllocator = new DynamicRowIDAllocator(session, databaseName, tableName,
        sinkOptions.getRowIdAllocatorStep(), start);
    this.tiDBEncodeHelper = new TiDBEncodeHelper(session, rowIDAllocator, tiTimestamp, databaseName,
        tableName, sinkOptions.isIgnoreAutoincrementColumn());
    this.tiTableInfo = tiDBEncodeHelper.getTiTableInfo();
    if (sinkOptions.getSinkTransaction() == GLOBAL) {
      this.tiDBWriteHelper = new TiDBWriteHelper(session.getTiSession(), tiTimestamp.getVersion(),
          primaryKey);
    }
    this.buffer = new ArrayList<>(sinkOptions.getBufferSize());
  }

  @Override
  public void close() throws Exception {
    if (session != null) {
      session.close();
    }
    Optional.ofNullable(tiDBWriteHelper).ifPresent(TiDBWriteHelper::close);
    Optional.ofNullable(tiDBEncodeHelper).ifPresent(TiDBEncodeHelper::close);
  }

  // TODO there are same elements in one batch?
  private void flushRows() {
    if (buffer.size() == 0) {
      return;
    }
    List<BytePairWrapper> pairs = new ArrayList<>(buffer.size());
    buffer.forEach(row -> pairs.addAll(tiDBEncodeHelper.generateKeyValuesByRow(row)));
    if (tiDBWriteHelper == null) {
      tiDBWriteHelper = new TiDBWriteHelper(session.getTiSession(), tiTimestamp.getVersion());
      tiDBWriteHelper.preWriteFirst(pairs);
      this.primaryKey = tiDBWriteHelper.getPrimaryKeyMust();
    } else {
      tiDBWriteHelper.preWriteSecondKeys(pairs);
    }
    this.buffer = new ArrayList<>(sinkOptions.getBufferSize());
  }

  @Override
  public void processElement(StreamRecord<Row> element) throws Exception {
    Row row = element.getValue();
    if (buffer.size() >= sinkOptions.getBufferSize()) {
      flushRows();
    }
    buffer.add(row);
  }

  @Override
  public void endInput() throws Exception {
    if (buffer.size() != 0) {
      flushRows();
    }
    if (sinkOptions.getSinkTransaction() == LOCAL && tiDBWriteHelper != null) {
      tiDBWriteHelper.commitPrimaryKey();
    }
  }


}
