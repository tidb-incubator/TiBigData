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

import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.RecordCursorInternal;
import io.tidb.bigdata.tidb.RecordSetInternal;
import io.tidb.bigdata.tidb.SplitInternal;
import io.tidb.bigdata.tidb.handle.ColumnHandleInternal;
import io.tidb.bigdata.tidb.types.DataType;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.meta.TiTimestamp;

public class TiDBRecordReader implements RecordReader<LongWritable, MapWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(TiDBRecordReader.class);

  private final TiDBInputSplit tidbInputSplit;
  private final Map<String, String> properties;
  private final List<SplitInternal> splitInternals;

  private long pos;
  private ClientSession clientSession;
  private Integer currentSplitIndex;
  private RecordCursorInternal currentCursor;
  private List<ColumnHandleInternal> columns;

  public TiDBRecordReader(InputSplit split, Map<String, String> properties) {
    this.currentSplitIndex = 0;
    this.tidbInputSplit = (TiDBInputSplit) split;
    this.properties = properties;
    this.splitInternals = tidbInputSplit.getSplitInternals();
  }

  private void initClientSession() {
    if (clientSession != null) {
      return;
    }
    try {
      LOG.info("Init client session");
      clientSession = ClientSession.create(new ClientConfig(properties));
    } catch (Exception e) {
      throw new IllegalStateException("Can not init client session", e);
    }
  }

  private void initCurrentCursor() {
    SplitInternal splitInternal = splitInternals.get(currentSplitIndex);

    columns =
        clientSession.getTableColumnsMust(
            splitInternal.getTable().getSchemaName(), splitInternal.getTable().getTableName());
    TiTimestamp timestamp =
        getOptionalVersion()
            .orElseGet(() -> getOptionalTimestamp().orElseGet(() -> splitInternal.getTimestamp()));
    RecordSetInternal recordSetInternal =
        new RecordSetInternal(
            clientSession,
            splitInternal,
            columns,
            Optional.empty(),
            Optional.ofNullable(timestamp));
    currentCursor = recordSetInternal.cursor();
  }

  @Override
  public boolean next(LongWritable longWritable, MapWritable mapWritable) throws IOException {
    initClientSession();

    if (currentCursor == null) {
      initCurrentCursor();
    }

    if (!currentCursor.advanceNextPosition()) {
      LOG.info("Current split index:" + currentSplitIndex);
      currentSplitIndex++;
      if (currentSplitIndex == splitInternals.size()) {
        return false;
      }
      initCurrentCursor();
      currentCursor.advanceNextPosition();
    }

    pos++;
    for (int i = 0; i < currentCursor.fieldCount(); i++) {
      ColumnHandleInternal column = columns.get(i);
      String name = column.getName();
      DataType type = column.getType();
      mapWritable.put(new Text(name), TypeUtils.toWriteable(currentCursor.getObject(i), type));
    }
    return true;
  }

  @Override
  public LongWritable createKey() {
    return new LongWritable();
  }

  @Override
  public MapWritable createValue() {
    return new MapWritable();
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public void close() throws IOException {
    try {
      currentCursor.close();
      clientSession.close();
    } catch (Exception e) {
      LOG.warn("Can not close session");
    }
  }

  @Override
  public float getProgress() throws IOException {
    return 0;
  }

  private Optional<TiTimestamp> getOptionalTimestamp() {
    return Optional.ofNullable(properties.get(ClientConfig.SNAPSHOT_TIMESTAMP))
        .filter(StringUtils::isNotEmpty)
        .map(s -> new TiTimestamp(Timestamp.from(ZonedDateTime.parse(s).toInstant()).getTime(), 0));
  }

  private Optional<TiTimestamp> getOptionalVersion() {
    return Optional.ofNullable(properties.get(ClientConfig.SNAPSHOT_VERSION))
        .filter(StringUtils::isNotEmpty)
        .map(Long::parseUnsignedLong)
        .map(tso -> new TiTimestamp(tso >> 18, tso & 0x3FFFF));
  }
}
