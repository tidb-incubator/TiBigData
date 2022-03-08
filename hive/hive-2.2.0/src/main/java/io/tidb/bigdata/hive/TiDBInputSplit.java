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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.tikv.common.meta.TiTimestamp;

public class TiDBInputSplit extends FileSplit {

  private Path path;
  private List<SplitInternal> splitInternals;

  public TiDBInputSplit() {
  }

  public TiDBInputSplit(Path path, List<SplitInternal> splitInternals) {
    this.path = path;
    this.splitInternals = splitInternals;
  }

  public List<SplitInternal> getSplitInternals() {
    return splitInternals;
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
    dataOutput.writeInt(splitInternals.size());
    for (int i = 0; i < splitInternals.size(); i++) {
      dataOutput.writeUTF(splitInternals.get(i).getTable().getSchemaName());
      dataOutput.writeUTF(splitInternals.get(i).getTable().getTableName());
      dataOutput.writeUTF(splitInternals.get(i).getStartKey());
      dataOutput.writeUTF(splitInternals.get(i).getEndKey());
      dataOutput.writeLong(splitInternals.get(i).getTimestamp().getPhysical());
      dataOutput.writeLong(splitInternals.get(i).getTimestamp().getLogical());
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.path = new Path(dataInput.readUTF());
    Integer size = dataInput.readInt();
    List<SplitInternal> splitInternalList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      String databaseName = dataInput.readUTF();
      String tableName = dataInput.readUTF();
      String startKey = dataInput.readUTF();
      String endKey = dataInput.readUTF();
      Long physicalTimestamp = dataInput.readLong();
      Long logicalTimestamp = dataInput.readLong();

      SplitInternal splitInternal = new SplitInternal(
          new TableHandleInternal(UUID.randomUUID().toString(), databaseName, tableName),
          startKey,
          endKey,
          new TiTimestamp(physicalTimestamp, logicalTimestamp));
      splitInternalList.add(splitInternal);
    }
    this.splitInternals = splitInternalList;
  }

}
