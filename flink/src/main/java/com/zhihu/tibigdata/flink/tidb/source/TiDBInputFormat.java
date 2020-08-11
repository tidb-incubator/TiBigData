/*
 * Copyright 2020 Zhihu.
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

package com.zhihu.tibigdata.flink.tidb.source;


import com.zhihu.tibigdata.flink.tidb.utils.DataTypeMappingUtil;
import com.zhihu.tibigdata.tidb.ClientConfig;
import com.zhihu.tibigdata.tidb.ClientSession;
import com.zhihu.tibigdata.tidb.ColumnHandleInternal;
import com.zhihu.tibigdata.tidb.RecordCursorInternal;
import com.zhihu.tibigdata.tidb.RecordSetInternal;
import com.zhihu.tibigdata.tidb.SplitInternal;
import com.zhihu.tibigdata.tidb.SplitManagerInternal;
import com.zhihu.tibigdata.tidb.TableHandleInternal;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiDBInputFormat extends RichInputFormat<Row, InputSplit> implements
    ResultTypeQueryable<Row> {

  static final Logger LOG = LoggerFactory.getLogger(TiDBInputFormat.class);

  public static String DATABASE_NAME = "tidb.database.name";

  public static String TABLE_NAME = "tidb.table.name";

  private final Properties properties;

  private final String databaseName;

  private final String tableName;

  private final String[] fieldNames;

  private final DataType[] fieldTypes;

  private final List<SplitInternal> splits;

  private final List<ColumnHandleInternal> columnHandleInternals;

  private transient RecordCursorInternal cursor;

  private transient ClientSession clientSession;

  public TiDBInputFormat(Properties properties, String[] fieldNames, DataType[] fieldTypes) {
    this.properties = Preconditions.checkNotNull(properties, "properties can not be null");
    this.databaseName = getRequiredProperties(DATABASE_NAME);
    this.tableName = getRequiredProperties(TABLE_NAME);
    this.fieldNames = fieldNames;
    this.fieldTypes = fieldTypes;
    // get split
    try (ClientSession splitSession = new ClientSession(new ClientConfig(properties))) {
      TableHandleInternal tableHandleInternal = new TableHandleInternal(
          UUID.randomUUID().toString(), this.databaseName, this.tableName);
      SplitManagerInternal splitManagerInternal = new SplitManagerInternal(splitSession);
      splits = splitManagerInternal.getSplits(tableHandleInternal);
      columnHandleInternals = splitSession.getTableColumns(tableHandleInternal)
          .orElseThrow(() -> new NullPointerException("columnHandleInternals is null"));
    } catch (Exception e) {
      throw new IllegalStateException("can not get split", e);
    }
  }

  @Override
  public void configure(Configuration parameters) {
    // do nothing here
  }

  @Override
  public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
    return cachedStatistics;
  }

  @Override
  public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
    GenericInputSplit[] inputSplits = new GenericInputSplit[splits.size()];
    for (int i = 0; i < inputSplits.length; i++) {
      inputSplits[i] = new GenericInputSplit(i, inputSplits.length);
    }
    return inputSplits;
  }

  @Override
  public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
    return new DefaultInputSplitAssigner(inputSplits);
  }

  @Override
  public void openInputFormat() throws IOException {
    clientSession = new ClientSession(new ClientConfig(properties));
  }

  @Override
  public void closeInputFormat() throws IOException {
    if (clientSession != null) {
      try {
        clientSession.close();
      } catch (Exception e) {
        LOG.warn("can not close clientSession", e);
      }
    }
  }

  @Override
  public void open(InputSplit split) throws IOException {
    SplitInternal splitInternal = splits.get(split.getSplitNumber());
    RecordSetInternal recordSetInternal = new RecordSetInternal(clientSession, splitInternal,
        columnHandleInternals, Optional.empty());
    cursor = recordSetInternal.cursor();
  }

  @Override
  public void close() throws IOException {
    if (cursor != null) {
      cursor.close();
    }
  }

  @Override
  public boolean reachedEnd() throws IOException {
    return !cursor.advanceNextPosition();
  }

  @Override
  public Row nextRecord(Row row) throws IOException {
    for (int i = 0; i < row.getArity(); i++) {
      DataType fieldType = fieldTypes[i];
      Object object = cursor.getObject(i);
      // data can be null here
      row.setField(i, DataTypeMappingUtil.getObjectWithDataType(object, fieldType).orElse(null));
    }
    return row;
  }

  @Override
  public TypeInformation<Row> getProducedType() {
    return new RowTypeInfo(TypeConversions.fromDataTypeToLegacyInfo(fieldTypes), fieldNames);
  }

  private String getRequiredProperties(String key) {
    return Preconditions.checkNotNull(properties.getProperty(key), key + " can not be null");
  }
}
