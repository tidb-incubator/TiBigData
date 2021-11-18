package io.tidb.bigdata.flink.connector.sink;

import io.tidb.bigdata.flink.connector.sink.TiDBSinkFunction.TiDBTransactionContext;
import java.io.IOException;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public class TiDBTransactionContextSerializer extends
    TypeSerializerSingleton<TiDBTransactionContext> {

  @Override
  public boolean isImmutableType() {
    return true;
  }

  @Override
  public TiDBTransactionContext createInstance() {
    return new TiDBTransactionContext();
  }

  @Override
  public TiDBTransactionContext copy(TiDBTransactionContext from) {
    return new TiDBTransactionContext();
  }

  @Override
  public TiDBTransactionContext copy(TiDBTransactionContext from, TiDBTransactionContext reuse) {
    return new TiDBTransactionContext();
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {

  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(TiDBTransactionContext record, DataOutputView target) throws IOException {

  }

  @Override
  public TiDBTransactionContext deserialize(DataInputView source) throws IOException {
    return new TiDBTransactionContext();
  }

  @Override
  public TiDBTransactionContext deserialize(TiDBTransactionContext reuse, DataInputView source)
      throws IOException {
    return new TiDBTransactionContext();
  }

  @Override
  public TypeSerializerSnapshot<TiDBTransactionContext> snapshotConfiguration() {
    return new TransactionContextSerializerSnapshot();
  }

  public static final class TransactionContextSerializerSnapshot
      extends SimpleTypeSerializerSnapshot<TiDBTransactionContext> {

    public TransactionContextSerializerSnapshot() {
      super(TiDBTransactionContextSerializer::new);
    }
  }
}
