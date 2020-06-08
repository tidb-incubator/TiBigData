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
package com.zhihu.flink.source.tidb;


import com.zhihu.presto.tidb.*;
import java.io.IOException;
import java.time.LocalTime;
import java.util.List;
import java.util.Optional;
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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

public class TiDBInputFormat extends RichInputFormat<Row, InputSplit> implements ResultTypeQueryable<Row> {

  private String pdAddresses;

  private String databaseName;

  private String tableName;

  private String[] fieldNames;

  private DataType[] fieldTypes;

  private List<SplitInternal> splits;

  private List<ColumnHandleInternal> columnHandleInternals;

  private transient RecordCursorInternal cursor;

  private transient ClientSession clientSession;


  /**
   * see {@link TiDBInputFormat#builder()}
   */
  private TiDBInputFormat() {
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
    clientSession = new ClientSession(new ClientConfig(pdAddresses));
  }

  @Override
  public void closeInputFormat() throws IOException {
    // TODO clientSession will auto close?
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
      Object object;
      // map datatype
      if (fieldType.equals(DataTypes.FLOAT())) {
        object = cursor.getFloat(i);
      } else if (fieldType.equals(DataTypes.DATE())) {
        object = cursor.getDate(i).toLocalDate();
      } else if (fieldType.equals(DataTypes.TIMESTAMP())) {
        object = cursor.getTimestamp(i).toLocalDateTime();
      } else if (fieldType.equals(DataTypes.TIME())) {
        object = LocalTime.ofNanoOfDay(cursor.getLong(i));
      } else if (fieldType.equals(DataTypes.STRING())) {
        object = cursor.getObject(i);
        if (object instanceof byte[]) {
          object = new String((byte[]) object);
        }
      } else if (fieldType.equals(DataTypes.BOOLEAN())) {
        long aLong = cursor.getLong(i);
        if (aLong == 0) {
          object = false;
        } else if (aLong == 1) {
          object = true;
        } else {
          throw new IllegalArgumentException("can not parse boolean");
        }
      } else {
        object = cursor.getObject(i);
      }
      row.setField(i, object);
    }
    return row;
  }

  @Override
  public TypeInformation<Row> getProducedType() {
    return new RowTypeInfo(TypeConversions.fromDataTypeToLegacyInfo(fieldTypes), fieldNames);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private final TiDBInputFormat format;

    private Builder() {
      format = new TiDBInputFormat();
    }

    public Builder setPdAddresses(String pdAddresses) {
      format.pdAddresses = pdAddresses;
      return this;
    }

    public Builder setDatabaseName(String databaseName) {
      format.databaseName = databaseName;
      return this;
    }

    public Builder setTableName(String tableName) {
      format.tableName = tableName;
      return this;
    }

    public Builder setFieldNames(String[] fieldNames) {
      format.fieldNames = fieldNames;
      return this;
    }

    public Builder setFieldTypes(DataType[] fieldTypes) {
      format.fieldTypes = fieldTypes;
      return this;
    }

    public TiDBInputFormat build() {
      ClientConfig clientConfig = new ClientConfig(format.pdAddresses);
      ClientSession clientSession = new ClientSession(clientConfig);
      TableHandleInternal tableHandleInternal = new TableHandleInternal(UUID.randomUUID().toString(),
          format.databaseName, format.tableName);
      SplitManagerInternal splitManagerInternal = new SplitManagerInternal(clientSession);
      format.splits = splitManagerInternal.getSplits(tableHandleInternal);
      format.columnHandleInternals = clientSession.getTableColumns(tableHandleInternal)
          .orElseThrow(() -> new NullPointerException("columnHandleInternals is null"));
      return format;
    }
  }
}
