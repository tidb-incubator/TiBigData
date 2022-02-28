package io.tidb.bigdata.flink.connector.sink.operator;

import io.tidb.bigdata.flink.connector.sink.TiDBSinkOptions;
import io.tidb.bigdata.tidb.RowBuffer;
import io.tidb.bigdata.tidb.TiDBEncodeHelper;
import io.tidb.bigdata.tidb.TiDBWriteHelper;
import io.tidb.bigdata.tidb.TiDBWriteMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.BytePairWrapper;
import org.tikv.common.meta.TiTimestamp;

public class TiDBGlobalWriteOperator extends TiDBWriteOperator {
  private static final Logger LOG = LoggerFactory.getLogger(TiDBGlobalWriteOperator.class);

  public TiDBGlobalWriteOperator(String databaseName, String tableName,
      Map<String, String> properties, TiTimestamp tiTimestamp,
      TiDBSinkOptions sinkOption, byte[] primaryKey,  List<Long> rowIdStarts) {
    super(databaseName, tableName, properties, tiTimestamp, sinkOption, primaryKey, rowIdStarts);
  }

  @Override
  protected void openInternal() {
    this.buffer = RowBuffer.createDefault(sinkOptions.getBufferSize());
    this.tiDBWriteHelper = new TiDBWriteHelper(session.getTiSession(), tiTimestamp.getVersion(),
        primaryKey);
    this.tiDBEncodeHelper = new TiDBEncodeHelper(
        session,
        tiTimestamp,
        databaseName,
        tableName,
        sinkOptions.isIgnoreAutoincrementColumn(),
        sinkOptions.getWriteMode() == TiDBWriteMode.UPSERT,
        rowIDAllocator);
  }

  @Override
  protected void flushRows() {
    if (buffer.size() == 0) {
      return;
    }
    List<BytePairWrapper> pairs = new ArrayList<>(buffer.size());

    buffer.getRows().forEach(row -> pairs.addAll(tiDBEncodeHelper.generateKeyValuesByRow(row)));
    tiDBWriteHelper.preWriteSecondKeys(pairs);

    buffer.clear();
  }
}