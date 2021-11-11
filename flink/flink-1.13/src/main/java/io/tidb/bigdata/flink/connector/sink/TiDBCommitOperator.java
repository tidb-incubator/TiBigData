package io.tidb.bigdata.flink.connector.sink;

import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.TiDBWriteHelper;
import java.util.Map;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class TiDBCommitOperator extends AbstractStreamOperator<Void> implements
    OneInputStreamOperator<Void, Void>, BoundedOneInput {

  private final Map<String, String> properties;
  private final long startTs;
  private final byte[] primaryKey;
  private ClientSession clientSession;
  private TiDBWriteHelper tiDBWriteHelper;

  public TiDBCommitOperator(Map<String, String> properties, long startTs, byte[] primaryKey) {
    this.properties = properties;
    this.startTs = startTs;
    this.primaryKey = primaryKey;
  }


  @Override
  public void open() throws Exception {
    this.clientSession = ClientSession.createWithSingleConnection(new ClientConfig(properties));
    this.tiDBWriteHelper = new TiDBWriteHelper(clientSession.getTiSession(), startTs, primaryKey);
  }

  @Override
  public void close() throws Exception {
    clientSession.close();
    clientSession = null;
  }

  @Override
  public void endInput() throws Exception {
    tiDBWriteHelper.commitPrimaryKey();
  }

  @Override
  public void processElement(StreamRecord<Void> streamRecord) throws Exception {

  }
}
