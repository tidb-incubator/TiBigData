/*
 * Copyright 2021 TiDB Project Authors.
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

package io.tidb.bigdata.mapreduce.tidb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class TiDBInputSplit extends InputSplit implements Writable {
  private String startKey;
  private String endKey;
  private String connectorId;
  private String schemaName;
  private String tableName;

  public static final String [] EMPTY_STRING_ARRAY = new String[0];

  public TiDBInputSplit() {

  }

  public TiDBInputSplit(String startKey, String endKey, String connectorId,
      String schemaName, String tableName) {
    this.startKey = startKey;
    this.endKey = endKey;
    this.connectorId = connectorId;
    this.schemaName = schemaName;
    this.tableName = tableName;
  }

  public String getStartKey() {
    return startKey;
  }

  public String getEndKey() {
    return endKey;
  }

  public String getConnectorId() {
    return connectorId;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public String getTableName() {
    return tableName;
  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public String[] getLocations() {
    return EMPTY_STRING_ARRAY;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(startKey);
    out.writeUTF(endKey);
    out.writeUTF(connectorId);
    out.writeUTF(schemaName);
    out.writeUTF(tableName);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.startKey = in.readUTF();
    this.endKey = in.readUTF();
    this.connectorId = in.readUTF();
    this.schemaName = in.readUTF();
    this.tableName = in.readUTF();
  }
}
