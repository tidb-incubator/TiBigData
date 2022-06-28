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
import io.tidb.bigdata.tidb.TiDBWriteHelper;
import io.tidb.bigdata.tidb.TiDBWriteMode;
import io.tidb.bigdata.tidb.buffer.RowBuffer;
import io.tidb.bigdata.tidb.codec.TiDBEncodeHelper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.tikv.common.BytePairWrapper;
import org.tikv.common.meta.TiTimestamp;

public class TiDBGlobalWriteOperator extends TiDBWriteOperator {

  public TiDBGlobalWriteOperator(
      String databaseName,
      String tableName,
      Map<String, String> properties,
      TiTimestamp tiTimestamp,
      TiDBSinkOptions sinkOption,
      byte[] primaryKey) {
    super(databaseName, tableName, properties, tiTimestamp, sinkOption, primaryKey);
  }

  @Override
  protected void openInternal() {
    this.buffer = RowBuffer.createDefault(sinkOptions.getBufferSize());
    this.tiDBWriteHelper =
        new TiDBWriteHelper(session.getTiSession(), tiTimestamp.getVersion(), primaryKey);
    this.tiDBEncodeHelper =
        new TiDBEncodeHelper(
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
