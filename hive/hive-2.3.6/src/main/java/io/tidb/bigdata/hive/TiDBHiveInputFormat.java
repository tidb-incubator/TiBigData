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

import static io.tidb.bigdata.hive.TiDBConstant.DATABASE_NAME;
import static io.tidb.bigdata.hive.TiDBConstant.EMPTY_STRING;
import static io.tidb.bigdata.hive.TiDBConstant.REGIONS_PER_SPLIT;
import static io.tidb.bigdata.hive.TiDBConstant.TABLE_NAME;

import com.google.common.collect.Lists;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.SplitInternal;
import io.tidb.bigdata.tidb.SplitManagerInternal;
import io.tidb.bigdata.tidb.handle.TableHandleInternal;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class TiDBHiveInputFormat implements InputFormat<LongWritable, MapWritable> {

  @Override
  public InputSplit[] getSplits(JobConf jobConf, int i) throws IOException {
    try (ClientSession clientSession =
        ClientSession.create(new ClientConfig(getProperties(jobConf)))) {
      String tableName =
          Objects.requireNonNull(jobConf.get(TABLE_NAME), TABLE_NAME + " can not be null");
      String databaseName =
          Objects.requireNonNull(jobConf.get(DATABASE_NAME), DATABASE_NAME + " can not be null");
      Integer regionNumPerSplit = jobConf.getInt(REGIONS_PER_SPLIT, 1);

      TableHandleInternal tableHandle =
          new TableHandleInternal(EMPTY_STRING, databaseName, tableName);
      Path path = FileInputFormat.getInputPaths(jobConf)[0];

      List<SplitInternal> splits = new SplitManagerInternal(clientSession).getSplits(tableHandle);
      List<List<SplitInternal>> splitPartition = Lists.partition(splits, regionNumPerSplit);

      return splitPartition
          .stream()
          .map(splitInternals -> new TiDBInputSplit(path, splitInternals))
          .toArray(TiDBInputSplit[]::new);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public RecordReader<LongWritable, MapWritable> getRecordReader(
      InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
    return new TiDBRecordReader(inputSplit, getProperties(jobConf));
  }

  private Map<String, String> getProperties(JobConf jobConf) {
    Map<String, String> properties = new HashMap<>();
    jobConf.iterator().forEachRemaining(e -> properties.put(e.getKey(), e.getValue()));
    return properties;
  }
}
