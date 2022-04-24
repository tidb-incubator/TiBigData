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

import io.tidb.bigdata.tidb.SplitInternal;
import io.tidb.bigdata.tidb.TableHandleInternal;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.tikv.common.meta.TiTimestamp;

public class TiDBInputSplit extends InputSplit implements Writable {

  private List<SplitInternal> splitInternals;

  public static final String[] EMPTY_STRING_ARRAY = new String[0];

  public TiDBInputSplit() {}

  public TiDBInputSplit(List<SplitInternal> splitInternals) {
    this.splitInternals = splitInternals;
  }

  public List<SplitInternal> getSplitInternals() {
    return splitInternals;
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
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(splitInternals.size());
    for (SplitInternal splitInternal : splitInternals) {
      dataOutput.writeUTF(splitInternal.getTable().getSchemaName());
      dataOutput.writeUTF(splitInternal.getTable().getTableName());
      dataOutput.writeUTF(splitInternal.getStartKey());
      dataOutput.writeUTF(splitInternal.getEndKey());
      dataOutput.writeLong(splitInternal.getTimestamp().getPhysical());
      dataOutput.writeLong(splitInternal.getTimestamp().getLogical());
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    int size = dataInput.readInt();
    List<SplitInternal> splitInternalList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      String databaseName = dataInput.readUTF();
      String tableName = dataInput.readUTF();
      String startKey = dataInput.readUTF();
      String endKey = dataInput.readUTF();
      long physicalTimestamp = dataInput.readLong();
      long logicalTimestamp = dataInput.readLong();

      SplitInternal splitInternal =
          new SplitInternal(
              new TableHandleInternal(UUID.randomUUID().toString(), databaseName, tableName),
              startKey,
              endKey,
              new TiTimestamp(physicalTimestamp, logicalTimestamp));
      splitInternalList.add(splitInternal);
    }
    this.splitInternals = splitInternalList;
  }
}
