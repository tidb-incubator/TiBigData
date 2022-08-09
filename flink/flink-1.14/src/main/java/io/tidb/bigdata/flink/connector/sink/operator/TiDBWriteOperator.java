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
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.TiDBWriteHelper;
import io.tidb.bigdata.tidb.buffer.RowBuffer;
import io.tidb.bigdata.tidb.codec.TiDBEncodeHelper;
import io.tidb.bigdata.tidb.meta.TiTableInfo;
import io.tidb.bigdata.tidb.row.Row;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Base operator for flushing rows to TiDB. When the row size is larger than the threshold, it will
 * flush the rows to TiDB.
 */
public abstract class TiDBWriteOperator extends AbstractStreamOperator<Void>
    implements OneInputStreamOperator<Row, Void>, BoundedOneInput {

  protected final String databaseName;
  protected final String tableName;
  private final Map<String, String> properties;
  protected final TiDBSinkOptions sinkOptions;

  protected byte[] primaryKey;

  protected transient ClientSession session;
  protected transient TiDBEncodeHelper tiDBEncodeHelper;
  protected transient TiDBWriteHelper tiDBWriteHelper;
  protected transient TiTableInfo tiTableInfo;
  protected transient RowBuffer buffer;

  public TiDBWriteOperator(
      String databaseName,
      String tableName,
      Map<String, String> properties,
      TiDBSinkOptions sinkOption,
      byte[] primaryKey) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.properties = properties;
    this.sinkOptions = sinkOption;
    this.primaryKey = primaryKey;
  }

  @Override
  public void open() throws Exception {
    this.session = ClientSession.create(new ClientConfig(properties));
    this.tiTableInfo = session.getTableMust(databaseName, tableName);
    openInternal();
  }

  protected abstract void openInternal();

  @Override
  public void close() throws Exception {
    if (session != null) {
      session.close();
    }
    Optional.ofNullable(tiDBWriteHelper).ifPresent(TiDBWriteHelper::close);
    Optional.ofNullable(tiDBEncodeHelper).ifPresent(TiDBEncodeHelper::close);
  }

  protected abstract void flushRows();

  @Override
  public void processElement(StreamRecord<Row> element) throws Exception {
    Row row = element.getValue();
    if (buffer.isFull()) {
      flushRows();
    }
    boolean added = buffer.add(row);
    if (!added && !sinkOptions.isDeduplicate()) {
      throw new IllegalStateException(
          "Duplicate index in one batch, please enable deduplicate, row = " + row);
    }
  }

  @Override
  public void endInput() throws Exception {
    if (buffer.size() != 0) {
      flushRows();
    }
  }
}
