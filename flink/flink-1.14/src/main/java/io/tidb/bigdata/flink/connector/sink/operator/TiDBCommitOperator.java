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

import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.TiDBWriteHelper;
import java.util.Map;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/** An operator to commit the primary key after the prewrite of secondary keys has been done. */
public class TiDBCommitOperator extends AbstractStreamOperator<Void>
    implements OneInputStreamOperator<Void, Void>, BoundedOneInput {

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
    this.clientSession = ClientSession.create(new ClientConfig(properties));
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
  public void processElement(StreamRecord<Void> streamRecord) throws Exception {}
}
