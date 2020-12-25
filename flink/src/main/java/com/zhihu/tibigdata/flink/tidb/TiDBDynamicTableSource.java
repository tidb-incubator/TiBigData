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

package com.zhihu.tibigdata.flink.tidb;

import static com.zhihu.tibigdata.flink.tidb.TiDBDynamicTableFactory.DATABASE_NAME;
import static com.zhihu.tibigdata.flink.tidb.TiDBDynamicTableFactory.TABLE_NAME;

import com.zhihu.tibigdata.tidb.ClientConfig;
import com.zhihu.tibigdata.tidb.ClientSession;
import com.zhihu.tibigdata.tidb.ColumnHandleInternal;
import com.zhihu.tibigdata.tidb.Expressions;
import com.zhihu.tibigdata.tidb.TableHandleInternal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.util.Preconditions;
import org.tikv.common.expression.Expression;
import org.tikv.common.types.DataType;

public class TiDBDynamicTableSource implements ScanTableSource, SupportsLimitPushDown,
    SupportsProjectionPushDown, SupportsFilterPushDown {

  private final TableSchema tableSchema;

  private final Map<String, String> properties;

  private final ClientConfig config;

  private long limit = Long.MAX_VALUE;

  private int[][] projectedFields;

  private Expression expression;

  private Map<String, DataType> nameTypeMap;

  public TiDBDynamicTableSource(TableSchema tableSchema, Map<String, String> properties) {
    this.tableSchema = tableSchema;
    this.properties = properties;
    this.config = new ClientConfig(properties);
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
    TypeInformation<RowData> typeInformation = runtimeProviderContext
        .createTypeInformation(tableSchema.toRowDataType());
    TiDBRowDataInputFormat tidbRowDataInputFormat = new TiDBRowDataInputFormat(properties,
        tableSchema.getFieldNames(), tableSchema.getFieldDataTypes(), typeInformation);
    tidbRowDataInputFormat.setLimit(limit);
    if (projectedFields != null) {
      tidbRowDataInputFormat.setProjectedFields(projectedFields);
    }
    if (expression != null) {
      tidbRowDataInputFormat.setExpression(expression);
    }
    return InputFormatProvider.of(tidbRowDataInputFormat);
  }

  @Override
  public DynamicTableSource copy() {
    TiDBDynamicTableSource tableSource = new TiDBDynamicTableSource(tableSchema, properties);
    tableSource.limit = this.limit;
    tableSource.projectedFields = this.projectedFields;
    tableSource.expression = this.expression;
    return tableSource;
  }

  @Override
  public String asSummaryString() {
    return this.getClass().getName();
  }

  @Override
  public void applyLimit(long limit) {
    this.limit = limit;
  }

  @Override
  public boolean supportsNestedProjection() {
    return false;
  }

  @Override
  public void applyProjection(int[][] projectedFields) {
    this.projectedFields = projectedFields;
  }

  @Override
  public Result applyFilters(List<ResolvedExpression> filters) {
    if (config.isFilterPushDown()) {
      this.expression = createExpression(filters);
    }
    return Result.of(Collections.emptyList(), filters);
  }

  private String getRequiredProperties(String key) {
    return Preconditions.checkNotNull(properties.get(key), key + " can not be null");
  }

  private void queryNameType() {
    String databaseName = getRequiredProperties(DATABASE_NAME.key());
    String tableName = getRequiredProperties(TABLE_NAME.key());
    try (ClientSession clientSession = ClientSession.createWithSingleConnection(config)) {
      TableHandleInternal tableHandleInternal = new TableHandleInternal(
          UUID.randomUUID().toString(), databaseName, tableName);
      this.nameTypeMap = clientSession.getTableColumns(tableHandleInternal)
          .orElseThrow(() -> new NullPointerException("columnHandleInternals is null"))
          .stream().collect(Collectors.toMap(ColumnHandleInternal::getName,
              ColumnHandleInternal::getType));
    } catch (Exception e) {
      throw new IllegalStateException("can not get columns", e);
    }
  }

  private Expression createExpression(List<ResolvedExpression> filters) {
    if (filters == null || filters.size() == 0) {
      return null;
    }
    if (nameTypeMap == null) {
      queryNameType();
    }
    return getExpression(filters);
  }

  private Expression getExpression(List<ResolvedExpression> resolvedExpressions) {
    return Expressions.and(resolvedExpressions.stream().map(this::getExpression));
  }

  private Expression getExpression(ResolvedExpression resolvedExpression) {
    if (resolvedExpression instanceof CallExpression) {
      CallExpression callExpression = (CallExpression) resolvedExpression;
      List<ResolvedExpression> resolvedChildren = callExpression.getResolvedChildren();
      switch (callExpression.getFunctionName()) {
        case "cast":
          // we only need column name
          return getExpression(resolvedChildren.get(0));
        case "or":
          // ignore always true expression
          return Expressions.or(resolvedChildren.stream().map(this::getExpression)
              .filter(exp -> exp != Expressions.alwaysTrue()));
        case "not":
          return Expressions.not(getExpression(resolvedChildren.get(0)));
        case "greaterThan":
          return Expressions.greaterThan(getExpression(resolvedChildren.get(0)),
              getExpression(resolvedChildren.get(1)));
        case "greaterThanOrEqual":
          return Expressions.greaterEqual(getExpression(resolvedChildren.get(0)),
              getExpression(resolvedChildren.get(1)));
        case "lessThan":
          return Expressions.lessThan(getExpression(resolvedChildren.get(0)),
              getExpression(resolvedChildren.get(1)));
        case "lessThanOrEqual":
          return Expressions.lessEqual(getExpression(resolvedChildren.get(0)),
              getExpression(resolvedChildren.get(1)));
        case "equals":
          return Expressions.equal(getExpression(resolvedChildren.get(0)),
              getExpression(resolvedChildren.get(1)));
        case "notEquals":
          return Expressions.notEqual(getExpression(resolvedChildren.get(0)),
              getExpression(resolvedChildren.get(1)));
        case "like":
          return Expressions.like(getExpression(resolvedChildren.get(0)),
              getExpression(resolvedChildren.get(1)));
        default:
          return Expressions.alwaysTrue();
      }
    }
    if (resolvedExpression instanceof FieldReferenceExpression) {
      String name = ((FieldReferenceExpression) resolvedExpression).getName();
      return Expressions.column(name, nameTypeMap.get(name));
    }
    if (resolvedExpression instanceof ValueLiteralExpression) {
      ValueLiteralExpression valueLiteralExpression = (ValueLiteralExpression) resolvedExpression;
      Object value = valueLiteralExpression
          .getValueAs(valueLiteralExpression.getOutputDataType().getConversionClass())
          .orElseThrow(() -> new IllegalStateException("can not get value"));
      return Expressions.constant(value, null);
    }
    return Expressions.alwaysTrue();
  }
}
