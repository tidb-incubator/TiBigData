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

package io.tidb.bigdata.hive;

import io.tidb.bigdata.tidb.SplitInternal;
import io.tidb.bigdata.tidb.TableHandleInternal;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.tikv.common.meta.TiTimestamp;

public class TiDBInputSplit extends FileSplit {

  private Path path;
  private String databaseName;
  private String tableName;
  private String startKey;
  private String endKey;
  private long physicalTimestamp;
  private long logicalTimestamp;


  public TiDBInputSplit() {
  }

  public TiDBInputSplit(Path path, SplitInternal splitInternal) {
    this(path,
        splitInternal.getTable().getSchemaName(),
        splitInternal.getTable().getTableName(),
        splitInternal.getStartKey(),
        splitInternal.getEndKey(),
        splitInternal.getTimestamp().getPhysical(),
        splitInternal.getTimestamp().getLogical());
  }

  public TiDBInputSplit(Path path, String databaseName, String tableName, String startKey,
      String endKey, long physicalTimestamp, long logicalTimestamp) {
    this.path = path;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.startKey = startKey;
    this.endKey = endKey;
    this.physicalTimestamp = physicalTimestamp;
    this.logicalTimestamp = logicalTimestamp;
  }

  public SplitInternal toInternal() {
    return new SplitInternal(
        new TableHandleInternal(UUID.randomUUID().toString(), databaseName, tableName),
        startKey,
        endKey,
        new TiTimestamp(physicalTimestamp, logicalTimestamp));
  }

  @Override
  public Path getPath() {
    return path;
  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException {
    return new String[0];
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(path.toString());
    dataOutput.writeUTF(databaseName);
    dataOutput.writeUTF(tableName);
    dataOutput.writeUTF(startKey);
    dataOutput.writeUTF(endKey);
    dataOutput.writeLong(physicalTimestamp);
    dataOutput.writeLong(logicalTimestamp);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.path = new Path(dataInput.readUTF());
    this.databaseName = dataInput.readUTF();
    this.tableName = dataInput.readUTF();
    this.startKey = dataInput.readUTF();
    this.endKey = dataInput.readUTF();
    this.physicalTimestamp = dataInput.readLong();
    this.logicalTimestamp = dataInput.readLong();
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getStartKey() {
    return startKey;
  }

  public String getEndKey() {
    return endKey;
  }

  public long getPhysicalTimestamp() {
    return physicalTimestamp;
  }

  public long getLogicalTimestamp() {
    return logicalTimestamp;
  }
}
