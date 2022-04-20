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

import io.tidb.bigdata.flink.connector.sink.function.TiDBSinkFunction.TiDBTransactionContext;
import java.io.IOException;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public class TiDBTransactionContextSerializer
    extends TypeSerializerSingleton<TiDBTransactionContext> {

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
  public void copy(DataInputView source, DataOutputView target) throws IOException {}

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(TiDBTransactionContext record, DataOutputView target) throws IOException {}

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
