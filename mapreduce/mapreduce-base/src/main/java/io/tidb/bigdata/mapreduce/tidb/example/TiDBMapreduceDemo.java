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
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class TiDBMapreduceDemo {

  public static void main(String[] args) {
    try {
      System.exit(run(args));
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  public static int run(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    MapreduceCmd cmd = new MapreduceCmd(args);

    Configuration conf = new Configuration();
    TiDBConfiguration.configureDB(conf,
        cmd.databaseUrl, cmd.databaseName, cmd.username, cmd.password);
    if (cmd.clusterTlsEnabled) {
      if (cmd.clusterUseJks) {
        TiDBConfiguration.clusterTlsJks(conf,
            cmd.clusterJksKeyPath,
            cmd.clusterJksKeyPassword,
            cmd.clusterJksTrustPath,
            cmd.clusterJksTrustPassword);
      } else {
        TiDBConfiguration.clusterTls(conf,
            cmd.clusterTlsCA, cmd.clusterTlsCert, cmd.clusterTlsKey);
      }
    }
    Job job = Job.getInstance(conf, "MRFormTiDB");
    TiDBInputFormat.setInput(job, TiDBRowData.class, cmd.tableName,
        cmd.fields.isEmpty() ? null : cmd.fields.toArray(new String[0]), cmd.limit, cmd.timestamp);
    job.setJarByClass(TiDBMapreduceDemo.class);
    job.setInputFormatClass(TiDBInputFormat.class);
    job.setMapperClass(TiDBMapper.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setNumReduceTasks(0);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class TiDBMapper extends
      Mapper<LongWritable, TiDBRowData, NullWritable, NullWritable> {
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

    private void printFields(String[] fileds) {
      StringBuilder sb = new StringBuilder();
      sb.append(String.format("%32s", "FIELDNAME"));
      System.out.println(toString(fileds, sb));
    }

    private void printRowData(long recordId, String row) {
      StringBuilder sb = new StringBuilder();
      sb.append(String.format("%32s", recordId));
      sb.append(String.format("%32s", row));
      System.out.println(sb.toString());
    }

    private String toString(Object[] row, StringBuilder sb) {
      for (Object column : row) {
        sb.append(String.format("%32s", column.toString()));
      }
      return sb.toString();
    }
  }


  public static class TiDBRowData implements TiDBWritable {

    private Integer c1;
    private Integer c2;
    private Integer c3;
    private Integer c4;
    private Long c5;
    private String c6;
    private String c7;
    private String c8;
    private String c9;
    private String c10;
    private String c11;
    private byte[] c12;
    private byte[] c13;
    private byte[] c14;
    private byte[] c15;
    private byte[] c16;
    private byte[] c17;
    private Float c18;
    private Double c19;
    private BigDecimal c20;
    private Date c21;
    private Time c22;
    private Timestamp c23;
    private Timestamp c24;
    private Integer c25;
    private Boolean c26;
    private String c27;
    private String c28;
    private String c29;


    public TiDBRowData() {

    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
      this.c1 = resultSet.getInt(1);
      this.c2 = resultSet.getInt(2);
      this.c3 = resultSet.getInt(3);
      this.c4 = resultSet.getInt(4);
      this.c5 = resultSet.getLong(5);
      this.c6 = resultSet.getString(6);
      this.c7 = resultSet.getString(7);
      this.c8 = resultSet.getString(8);
      this.c9 = resultSet.getString(9);
      this.c10 = resultSet.getString(10);
      this.c11 = resultSet.getString(11);
      this.c12 = resultSet.getBytes(12);
      this.c13 = resultSet.getBytes(13);
      this.c14 = resultSet.getBytes(14);
      this.c15 = resultSet.getBytes(15);
      this.c16 = resultSet.getBytes(16);
      this.c17 = resultSet.getBytes(17);
      this.c18 = resultSet.getFloat(18);
      this.c19 = resultSet.getDouble(19);
      this.c20 = resultSet.getBigDecimal(20);
      this.c21 = resultSet.getDate(21);
      this.c22 = resultSet.getTime(22);
      this.c23 = resultSet.getTimestamp(23);
      this.c24 = resultSet.getTimestamp(24);
      this.c25 = resultSet.getInt(25);
      this.c26 = resultSet.getBoolean(26);
      this.c27 = resultSet.getString(27);
      this.c28 = resultSet.getString(28);
      this.c29 = resultSet.getString(29);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(String.format("%32s", c1));
      sb.append(String.format("%32s", c2));
      sb.append(String.format("%32s", c3));
      sb.append(String.format("%32s", c4));
      sb.append(String.format("%32s", c5));
      sb.append(String.format("%32s", c6));
      sb.append(String.format("%32s", c7));
      sb.append(String.format("%32s", c8));
      sb.append(String.format("%32s", c9));
      sb.append(String.format("%32s", c10));
      sb.append(String.format("%32s", c11));
      sb.append(String.format("%32s", new String(c12)));
      sb.append(String.format("%32s", new String(c13)));
      sb.append(String.format("%32s", new String(c14)));
      sb.append(String.format("%32s", new String(c15)));
      sb.append(String.format("%32s", new String(c16)));
      sb.append(String.format("%32s", new String(c17)));
      sb.append(String.format("%32s", c18));
      sb.append(String.format("%32s", c19));
      sb.append(String.format("%32s", c20));
      sb.append(String.format("%32s", c21));
      sb.append(String.format("%32s", c22));
      sb.append(String.format("%32s", c23));
      sb.append(String.format("%32s", c24));
      sb.append(String.format("%32s", c25));
      sb.append(String.format("%32s", c26));
      sb.append(String.format("%32s", c27));
      sb.append(String.format("%32s", c28));
      sb.append(String.format("%32s", c29));
      return sb.toString();
    }
  }
}
