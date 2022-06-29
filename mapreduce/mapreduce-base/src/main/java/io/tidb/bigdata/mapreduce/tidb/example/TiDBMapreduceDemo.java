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

package io.tidb.bigdata.mapreduce.tidb.example;

import io.tidb.bigdata.mapreduce.tidb.TiDBConfiguration;
import io.tidb.bigdata.mapreduce.tidb.TiDBInputFormat;
import io.tidb.bigdata.mapreduce.tidb.TiDBWritable;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class TiDBMapreduceDemo {

  public static void main(String[] args) {
    try {
      Job job = createJob(args);
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  public static Job createJob(String[] args) throws IOException {
    MapreduceCmd cmd = new MapreduceCmd(args);

    Configuration conf = new Configuration();
    TiDBConfiguration.configureDB(
        conf, cmd.databaseUrl, cmd.databaseName, cmd.username, cmd.password);
    if (cmd.clusterTlsEnabled) {
      if (cmd.clusterUseJks) {
        TiDBConfiguration.clusterTlsJks(
            conf,
            cmd.clusterJksKeyPath,
            cmd.clusterJksKeyPassword,
            cmd.clusterJksTrustPath,
            cmd.clusterJksTrustPassword);
      } else {
        TiDBConfiguration.clusterTls(conf, cmd.clusterTlsCA, cmd.clusterTlsCert, cmd.clusterTlsKey);
      }
    }
    Job job = Job.getInstance(conf, "MRFormTiDB");
    TiDBInputFormat.setInput(
        job,
        TiDBRowData.class,
        cmd.tableName,
        cmd.fields.isEmpty() ? null : cmd.fields.toArray(new String[0]),
        cmd.limit,
        cmd.timestamp);
    job.setJarByClass(TiDBMapreduceDemo.class);
    job.setInputFormatClass(TiDBInputFormat.class);
    job.setMapperClass(TiDBMapper.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setNumReduceTasks(0);
    return job;
  }

  public static class TiDBMapper
      extends Mapper<LongWritable, TiDBRowData, NullWritable, NullWritable> {

    @Override
    protected void setup(Context context) {
      System.out.println("job attempt ID : " + context.getTaskAttemptID());
      printFields(context.getConfiguration().getStrings("tidb.field.names"));
    }

    @Override
    protected void map(LongWritable key, TiDBRowData value, Context context) {
      // do mapper here
      printRowData(key.get(), value.toString());
    }

    private void printFields(String[] fields) {
      StringBuilder sb = new StringBuilder();
      sb.append(String.format("%32s", "FIELDNAME"));
      for (String field : fields) {
        sb.append(String.format("%32s", field));
      }
      System.out.println(sb);
    }

    private void printRowData(long recordId, String row) {
      StringBuilder sb = new StringBuilder();
      sb.append(String.format("%32s", recordId));
      sb.append(String.format("%32s", row));
      System.out.println(sb);
    }
  }

  public static class TiDBRowData implements TiDBWritable {

    private String[] fieldNames;
    private Object[] values;

    public TiDBRowData() {}

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
      ResultSetMetaData metaData = resultSet.getMetaData();
      int columnCount = metaData.getColumnCount();
      fieldNames = new String[columnCount];
      values = new Object[columnCount];
      for (int i = 1; i <= columnCount; i++) {
        final int fieldIndex = i - 1;
        fieldNames[fieldIndex] = metaData.getColumnName(i);
        String className = metaData.getColumnClassName(i);
        TiDBJdbcClassName tiDBJdbcClassName = TiDBJdbcClassName.fromClassName(className);
        switch (tiDBJdbcClassName) {
          case BOOLEAN:
            values[fieldIndex] = resultSet.getBoolean(i);
            break;
          case BYTE:
          case SHORT:
          case INTEGER:
            values[fieldIndex] = resultSet.getInt(i);
            break;
          case LONG:
            values[fieldIndex] = resultSet.getLong(i);
            break;
          case FLOAT:
            values[fieldIndex] = resultSet.getFloat(i);
            break;
          case BIGDECIMAL:
            values[fieldIndex] = resultSet.getBigDecimal(i);
            break;
          case DOUBLE:
            values[fieldIndex] = resultSet.getDouble(i);
            break;
          case TIME:
            values[fieldIndex] = resultSet.getTime(i);
            break;
          case TIMESTAMP:
          case DATETIME:
            values[fieldIndex] = resultSet.getTimestamp(i);
            break;
          case DATE:
            if (metaData.getColumnTypeName(i).equals("YEAR")) {
              values[fieldIndex] = resultSet.getInt(i);
            } else {
              values[fieldIndex] = resultSet.getDate(i);
            }
            break;
          case STRING:
            values[fieldIndex] = resultSet.getString(i);
            break;
          case BYTES:
            values[fieldIndex] = resultSet.getBytes(i);
            break;
          default:
            throw new IllegalArgumentException("Unsupported class name: " + className);
        }
      }
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      for (Object value : values) {
        sb.append(
            String.format("%32s", value instanceof byte[] ? new String((byte[]) value) : value));
      }
      return sb.toString();
    }
  }

  public enum TiDBJdbcClassName {
    BOOLEAN(Boolean.class.getName()),
    BYTE(Byte.class.getName()),
    SHORT(Short.class.getName()),
    INTEGER(Integer.class.getName()),
    LONG(Long.class.getName()),
    FLOAT(Float.class.getName()),
    DOUBLE(Double.class.getName()),
    BIGINTEGER(BigInteger.class.getName()),
    BIGDECIMAL(BigDecimal.class.getName()),
    STRING(String.class.getName()),
    BYTES(byte[].class.getName()),
    DATE(Date.class.getName()),
    DATETIME(LocalDateTime.class.getName()),
    TIMESTAMP(Timestamp.class.getName()),
    TIME(Time.class.getName()),
    ARRAY(Array.class.getName()),
    MAP(Map.class.getName());

    private final String className;

    TiDBJdbcClassName(String className) {
      this.className = className;
    }

    public static TiDBJdbcClassName fromClassName(String className) {
      return Arrays.stream(values())
          .filter(value -> value.className.equals(className))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("Invalid class name: " + className));
    }
  }
}
