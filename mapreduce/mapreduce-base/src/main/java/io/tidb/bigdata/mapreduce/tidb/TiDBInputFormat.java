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

import static io.tidb.bigdata.mapreduce.tidb.TiDBConfiguration.REGIONS_PER_SPLIT;
import static io.tidb.bigdata.mapreduce.tidb.TiDBConfiguration.REGIONS_PER_SPLIT_DEFAULT;

import com.google.common.collect.Lists;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.SplitInternal;
import io.tidb.bigdata.tidb.SplitManagerInternal;
import io.tidb.bigdata.tidb.handle.ColumnHandleInternal;
import io.tidb.bigdata.tidb.handle.TableHandleInternal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * A InputFormat that reads input data from an TiDB table.
 * <p>
 * DBInputFormat emits LongWritables containing the record number as key and TiDBWritables as
 * value.
 * <p>
 */
public class TiDBInputFormat<T extends TiDBWritable> extends InputFormat<LongWritable, T>
    implements Configurable {

  private List<ColumnHandleInternal> columnHandleInternals;

  private ClientSession clientSession;

  private TableHandleInternal tableHandleInternal;

  private TiDBConfiguration dbConf;

  private ResultSetMetaData resultSetMetaData;

  public TiDBInputFormat() {}

  /**
   * {@inheritDoc}
   *
   * @return
   */
  @Override
  public List<InputSplit> getSplits(JobContext job) {
    SplitManagerInternal splitManagerInternal = new SplitManagerInternal(clientSession);
    List<SplitInternal> splitInternals = splitManagerInternal.getSplits(tableHandleInternal);
    int regionsPerSplit = job.getConfiguration()
        .getInt(REGIONS_PER_SPLIT, REGIONS_PER_SPLIT_DEFAULT);
    return Lists.partition(splitInternals, regionsPerSplit).stream()
        .map(TiDBInputSplit::new)
        .collect(Collectors.toList());
  }

  @Override
  public RecordReader<LongWritable, T> createRecordReader(
      InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {

    return new TiDBRecordReader(
        (TiDBInputSplit) inputSplit,
        getConf(),
        getClientSession(),
        columnHandleInternals,
        resultSetMetaData);
  }

  @Override
  public void setConf(Configuration conf) {
    this.dbConf = new TiDBConfiguration(conf);
    getClientSession();

    String databaseName = dbConf.getDatabaseName();

    String tableName = dbConf.getInputTableName();

    // check database and table
    clientSession.getTableMust(databaseName, tableName);

    this.tableHandleInternal = new TableHandleInternal(
        UUID.randomUUID().toString(), databaseName, tableName);

    String[] fieldNames =
        Arrays.stream(dbConf.getInputFieldNames()).map(String::toLowerCase).toArray(String[]::new);

    if (1 == fieldNames.length && "*".equals(fieldNames[0])) {
      this.columnHandleInternals = clientSession.getTableColumnsMust(databaseName, tableName);
      fieldNames = columnHandleInternals.stream().map(ColumnHandleInternal::getName).collect(
          Collectors.toList()).toArray(new String[columnHandleInternals.size()]);
      dbConf.setInputFieldNames(fieldNames);
    } else {
      this.columnHandleInternals = clientSession.getTableColumns(
              tableHandleInternal, Arrays.asList(fieldNames))
          .orElseThrow(() -> new IllegalStateException("Can not get columns"));
    }

    conf.setStrings(
        "tidb.field.names",
        columnHandleInternals
            .stream()
            .map(ColumnHandleInternal::getName)
            .collect(Collectors.toList())
            .toArray(new String[columnHandleInternals.size()]));

    try (Connection con = dbConf.getJdbcConnection()) {
      String sql = "SELECT "
          + Arrays.stream(fieldNames).map(this::quote).collect(Collectors.joining(","))
          + " FROM "
          + quote(databaseName)
          + "."
          + quote(tableName)
          + " LIMIT 1";
      try (PreparedStatement ps = con.prepareStatement(sql)) {
        this.resultSetMetaData = ps.executeQuery().getMetaData();
      }
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  private String quote(String s) {
    return "`" + s + "`";
  }

  @Override
  public Configuration getConf() {
    return dbConf.getConf();
  }

  public TiDBConfiguration getTiDBConf() {
    return dbConf;
  }

  /**
   * Initializes the map-part of the job with the appropriate input settings.
   *
   * @param job        The map-reduce job
   * @param inputClass the class object implementing TiDBWritable, which is the Java object holding
   *                   tuple fields.
   * @param tableName  The table to read data from
   * @param fieldNames The field names in the table
   * @param limit      the limit of per mapper read record
   * @param snapshot   snapshot time
   * @see #setInput(Job, Class, String, String[], java.lang.Integer, String)
   */
  public static void setInput(
      Job job,
      Class<? extends TiDBWritable> inputClass,
      String tableName,
      String[] fieldNames,
      Integer limit,
      String snapshot) {
    TiDBConfiguration dbConf = new TiDBConfiguration(job.getConfiguration());
    dbConf.setInputClass(inputClass);
    dbConf.setInputTableName(tableName);
    if (null == fieldNames || 0 == fieldNames.length) {
      dbConf.setInputFieldNames(new String[]{"*"});
    } else {
      dbConf.setInputFieldNames(fieldNames);
    }
    if (null != limit) {
      dbConf.setMapperRecordLimit(limit);
    }
    if (null != snapshot) {
      dbConf.setSnapshot(snapshot);
    }
  }

  private ClientSession getClientSession() {
    if (null == this.clientSession) {
      this.clientSession = dbConf.getTiDBConnection();
    }

    return clientSession;
  }
}
