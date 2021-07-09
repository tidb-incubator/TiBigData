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

import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.ColumnHandleInternal;
import io.tidb.bigdata.tidb.RecordCursorInternal;
import io.tidb.bigdata.tidb.RecordSetInternal;
import io.tidb.bigdata.tidb.SplitInternal;
import io.tidb.bigdata.tidb.TableHandleInternal;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.meta.TiTimestamp;

/**
 * A RecordReader that reads records from a TiDB table.
 */
public class TiDBRecordReader<T extends TiDBWritable> extends
    RecordReader<LongWritable, T> {

  private static final Logger LOG = LoggerFactory.getLogger(TiDBRecordReader.class);

  private LongWritable key = null;

  private T value = null;

  private Class<T> inputClass;

  private TiDBConfiguration dfConf;

  private ClientSession clientSession;

  private RecordCursorInternal cursor;

  private int[] projectedFieldIndexes;

  private final List<ColumnHandleInternal> columnHandleInternals;

  private long limit;

  private TiTimestamp timestamp;

  private SplitInternal splitInternal;

  private long recordCount;

  private ResultSetMetaData resultSetMetaData;

  private TiDBResultSet tiDBResultSet;

  public TiDBRecordReader(TiDBInputSplit split, Configuration conf,
      ClientSession clientSession, List<ColumnHandleInternal> columnHandleInternals,
      ResultSetMetaData resultSetMetaData) {

    this.dfConf = new TiDBConfiguration(conf);
    this.inputClass = (Class<T>) dfConf.getInputClass();
    this.columnHandleInternals = columnHandleInternals;
    this.splitInternal = new SplitInternal(
        new TableHandleInternal(split.getConnectorId(), split.getSchemaName(),
            split.getTableName()), split.getStartKey(), split.getEndKey(),
        clientSession.getTimestamp());
    this.clientSession = clientSession;
    this.projectedFieldIndexes = IntStream.range(0, dfConf.getInputFieldNames().length).toArray();
    this.timestamp = Optional
        .ofNullable(dfConf.getSnapshot())
        .filter(StringUtils::isNoneEmpty)
        .map(s -> new TiTimestamp(Timestamp.from(ZonedDateTime.parse(s).toInstant()).getTime(), 0))
        .orElse(null);
    this.limit = dfConf.getMapperRecordLimit();
    this.recordCount = 0;
    this.resultSetMetaData = resultSetMetaData;
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
    // do nothing
  }

  @Override
  public boolean nextKeyValue() {
    if (key == null) {
      key = new LongWritable();
    }

    if (value == null) {
      RecordSetInternal recordSetInternal = new RecordSetInternal(clientSession, splitInternal,
          Arrays.stream(projectedFieldIndexes).mapToObj(columnHandleInternals::get)
              .collect(Collectors.toList()),
          Optional.empty(),
          Optional.ofNullable(timestamp),
          Optional.of(limit > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) limit));
      cursor = recordSetInternal.cursor();
      if (!cursor.advanceNextPosition()) {
        return false;
      }
      this.tiDBResultSet = new TiDBResultSet(cursor.fieldCount(), resultSetMetaData);
      value = ReflectionUtils.newInstance(inputClass, dfConf.getConf());
    } else {
      if (!cursor.advanceNextPosition()) {
        return false;
      }
    }

    key.set(recordCount++);

    try {
      updateTiDBResultSet(cursor);
      value.readFields(tiDBResultSet);
    } catch (SQLException e) {
      LOG.error("error when read record cursor.", e);
      return false;
    }
    return true;
  }

  public void updateTiDBResultSet(RecordCursorInternal cursor) {
    for (int index = 0; index < cursor.fieldCount(); index++) {
      Object object = cursor.getObject(index);
      tiDBResultSet.setObject(object, index + 1);
    }
  }

  @Override
  public LongWritable getCurrentKey() {
    return key;
  }

  @Override
  public T getCurrentValue() {
    return value;
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public void close() {
    if (clientSession != null) {
      try {
        clientSession.close();
        clientSession = null;
      } catch (Exception e) {
        LOG.warn("can not close clientSession", e);
      }
    }
    if (cursor != null) {
      cursor.close();
      cursor = null;
    }
  }
}
