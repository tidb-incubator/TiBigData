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

package com.zhihu.flink.tidb.source;


import com.zhihu.flink.tidb.utils.DataTypeMappingUtil;
import com.zhihu.presto.tidb.ClientConfig;
import com.zhihu.presto.tidb.ClientSession;
import com.zhihu.presto.tidb.ColumnHandleInternal;
import com.zhihu.presto.tidb.RecordCursorInternal;
import com.zhihu.presto.tidb.RecordSetInternal;
import com.zhihu.presto.tidb.SplitInternal;
import com.zhihu.presto.tidb.SplitManagerInternal;
import com.zhihu.presto.tidb.TableHandleInternal;
import java.io.IOException;
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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiDBInputFormat extends RichInputFormat<Row, InputSplit> implements
    ResultTypeQueryable<Row> {

  static final Logger LOG = LoggerFactory.getLogger(TiDBInputFormat.class);

  private final String pdAddresses;

  private final String databaseName;

  private final String tableName;

  private final String[] fieldNames;

  private final DataType[] fieldTypes;

  private final List<SplitInternal> splits;

  private final List<ColumnHandleInternal> columnHandleInternals;

  private transient RecordCursorInternal cursor;

  private transient ClientSession clientSession;


  /**
   * see {@link TiDBInputFormat#builder()}
   */
  private TiDBInputFormat(String pdAddresses, String databaseName, String tableName,
      String[] fieldNames, DataType[] fieldTypes) {
    this.pdAddresses = pdAddresses;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.fieldNames = fieldNames;
    this.fieldTypes = fieldTypes;
    List<SplitInternal> splits = null;
    List<ColumnHandleInternal> columnHandleInternals = null;
    // get split
    try (ClientSession splitSession = new ClientSession(new ClientConfig(this.pdAddresses))) {
      TableHandleInternal tableHandleInternal = new TableHandleInternal(
          UUID.randomUUID().toString(),
          this.databaseName, this.tableName);
      SplitManagerInternal splitManagerInternal = new SplitManagerInternal(splitSession);
      splits = splitManagerInternal.getSplits(tableHandleInternal);
      columnHandleInternals = splitSession.getTableColumns(tableHandleInternal)
          .orElseThrow(() -> new NullPointerException("columnHandleInternals is null"));
    } catch (Exception e) {
      LOG.error("can not get split", e);
      System.exit(1);
    }
    this.splits = splits;
    this.columnHandleInternals = columnHandleInternals;
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

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private String pdAddresses;

    private String databaseName;

    private String tableName;

    private String[] fieldNames;

    private DataType[] fieldTypes;

    private Builder() {

    }

    public Builder setPdAddresses(String pdAddresses) {
      this.pdAddresses = pdAddresses;
      return this;
    }

    public Builder setDatabaseName(String databaseName) {
      this.databaseName = databaseName;
      return this;
    }

    public Builder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder setFieldNames(String[] fieldNames) {
      this.fieldNames = fieldNames;
      return this;
    }

    public Builder setFieldTypes(DataType[] fieldTypes) {
      this.fieldTypes = fieldTypes;
      return this;
    }

    public TiDBInputFormat build() {
      return new TiDBInputFormat(pdAddresses, databaseName, tableName, fieldNames.clone(),
          fieldTypes.clone());
    }
  }
}
