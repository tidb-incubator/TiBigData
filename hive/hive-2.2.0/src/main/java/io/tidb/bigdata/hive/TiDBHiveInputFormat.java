package io.tidb.bigdata.hive;

import static io.tidb.bigdata.hive.TiDBConstant.DATABASE_NAME;
import static io.tidb.bigdata.hive.TiDBConstant.EMPTY_STRING;
import static io.tidb.bigdata.hive.TiDBConstant.TABLE_NAME;

import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.SplitManagerInternal;
import io.tidb.bigdata.tidb.TableHandleInternal;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class TiDBHiveInputFormat implements InputFormat<LongWritable, MapWritable> {

  @Override
  public InputSplit[] getSplits(JobConf jobConf, int i) throws IOException {
    try (ClientSession clientSession = ClientSession.create(
        new ClientConfig(getProperties(jobConf)))) {
      String tableName = Objects.requireNonNull(jobConf.get(TABLE_NAME),
          TABLE_NAME + " can not be null");
      String databaseName = Objects.requireNonNull(jobConf.get(DATABASE_NAME),
          DATABASE_NAME + " can not be null");
      TableHandleInternal tableHandle = new TableHandleInternal(EMPTY_STRING, databaseName,
          tableName);
      return new SplitManagerInternal(clientSession)
          .getSplits(tableHandle)
          .stream()
          .map(TiDBInputSplit::new)
          .toArray(InputSplit[]::new);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public RecordReader<LongWritable, MapWritable> getRecordReader(InputSplit inputSplit,
      JobConf jobConf, Reporter reporter) throws IOException {
    return new TiDBRecordReader(inputSplit, getProperties(jobConf));
  }

  private Map<String, String> getProperties(JobConf jobConf) {
    Map<String, String> properties = new HashMap<>();
    jobConf.iterator().forEachRemaining(e -> properties.put(e.getKey(), e.getValue()));
    return properties;
  }
}
