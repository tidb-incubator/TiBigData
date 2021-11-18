package io.tidb.bigdata.flink.connector.sink;

import io.tidb.bigdata.flink.connector.sink.TiDBSinkFunction.TiDBTransactionState;
import java.io.IOException;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public class TiDBTransactionStateSerializer extends TypeSerializerSingleton<TiDBTransactionState> {

  @Override
  public boolean isImmutableType() {
    return true;
  }

  @Override
  public TiDBTransactionState createInstance() {
    return null;
  }

  @Override
  public TiDBTransactionState copy(TiDBTransactionState from) {
    return new TiDBTransactionState(from.getTransactionId(), from.getPhysicalTs(),
        from.getLogicalTs());
  }

  @Override
  public TiDBTransactionState copy(TiDBTransactionState from, TiDBTransactionState reuse) {
    return from;
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    serialize(deserialize(source), target);
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(TiDBTransactionState record, DataOutputView target) throws IOException {
    target.writeUTF(record.getTransactionId());
    target.writeLong(record.getPhysicalTs());
    target.writeLong(record.getLogicalTs());
  }

  @Override
  public TiDBTransactionState deserialize(DataInputView source) throws IOException {
    return new TiDBTransactionState(source.readUTF(), source.readLong(), source.readLong());
  }

  @Override
  public TiDBTransactionState deserialize(TiDBTransactionState reuse, DataInputView source)
      throws IOException {
    return deserialize(source);
  }

  @Override
  public TypeSerializerSnapshot<TiDBTransactionState> snapshotConfiguration() {
    return new TransactionStateSerializerSnapshot();
  }

  public static final class TransactionStateSerializerSnapshot
      extends SimpleTypeSerializerSnapshot<TiDBTransactionState> {

    public TransactionStateSerializerSnapshot() {
      super(TiDBTransactionStateSerializer::new);
    }
  }
}
