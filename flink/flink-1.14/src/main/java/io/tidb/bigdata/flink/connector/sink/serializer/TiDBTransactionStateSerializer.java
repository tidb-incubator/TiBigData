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

package io.tidb.bigdata.flink.connector.sink.serializer;

import io.tidb.bigdata.flink.connector.sink.function.TiDBSinkFunction.TiDBTransactionState;
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
    return new TiDBTransactionState(
        from.getTransactionId(), from.getPhysicalTs(), from.getLogicalTs());
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
