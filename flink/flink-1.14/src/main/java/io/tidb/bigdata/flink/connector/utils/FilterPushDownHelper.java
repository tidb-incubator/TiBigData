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

package io.tidb.bigdata.flink.connector.utils;

import static io.tidb.bigdata.flink.connector.utils.FilterPushDownHelper.FlinkExpression.cast;
import static io.tidb.bigdata.flink.connector.utils.FilterPushDownHelper.FlinkExpression.equals;
import static io.tidb.bigdata.flink.connector.utils.FilterPushDownHelper.FlinkExpression.greaterThan;
import static io.tidb.bigdata.flink.connector.utils.FilterPushDownHelper.FlinkExpression.greaterThanOrEqual;
import static io.tidb.bigdata.flink.connector.utils.FilterPushDownHelper.FlinkExpression.lessThan;
import static io.tidb.bigdata.flink.connector.utils.FilterPushDownHelper.FlinkExpression.lessThanOrEqual;
import static io.tidb.bigdata.flink.connector.utils.FilterPushDownHelper.FlinkExpression.like;
import static io.tidb.bigdata.flink.connector.utils.FilterPushDownHelper.FlinkExpression.notEquals;

import com.google.common.collect.ImmutableSet;
import io.tidb.bigdata.tidb.Expressions;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import  io.tidb.bigdata.tidb.expression.Expression;
import  io.tidb.bigdata.tidb.expression.visitor.SupportedExpressionValidator;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.types.MySQLType;
import org.tikv.common.types.StringType;

public class FilterPushDownHelper {

  private static final Logger LOG = LoggerFactory.getLogger(FilterPushDownHelper.class);

  private static final Set<FlinkExpression> COMPARISON_BINARY_FILTERS = ImmutableSet.of(
      greaterThan,
      greaterThanOrEqual,
      lessThan,
      lessThanOrEqual,
      equals,
      notEquals,
      like
  );

  private static final Set<DataType> INTEGER_TYPES = ImmutableSet.of(
      DataTypes.TINYINT(),
      DataTypes.SMALLINT(),
      DataTypes.INT(),
      DataTypes.BIGINT()
  );

  private final TiTableInfo tiTableInfo;
  private final Map<String, org.tikv.common.types.DataType> nameTypeMap;
  private final Optional<StoreVersion> minimumTiKVVersion;

  public boolean isSupportEnumPushDown() {
    // enum push down is only supported when TiKV version >= 5.1.0
    return this.minimumTiKVVersion.map(storeVersion -> storeVersion.greatThan(new StoreVersion("5.1.0")))
        .orElse(false);
  }

  public FilterPushDownHelper(TiTableInfo tiTableInfo, List<StoreVersion> tiKVVersions) {
    this.tiTableInfo = tiTableInfo;
    this.nameTypeMap = tiTableInfo.getColumns().stream()
        .collect(Collectors.toMap(TiColumnInfo::getName, TiColumnInfo::getType));

    this.minimumTiKVVersion = tiKVVersions.stream().reduce((a, b) -> a.greatThan(b) ? b : a);
  }

  public Optional<Expression> toTiDBExpression(List<ResolvedExpression> filters) {
    if (filters == null || filters.size() == 0) {
      return Optional.empty();
    }
    return and(filters);
  }


  /**
   * Convert flink expression to tidb expression.
   * <p>
   * the BETWEEN, NOT_BETWEEN, IN expression will be converted by flink automatically. the BETWEEN
   * will be converted to (GT_EQ AND LT_EQ), the NOT_BETWEEN will be converted to (LT_EQ OR GT_EQ),
   * the IN will be converted to OR, so we do not add the conversion here
   *
   * @param resolvedExpression the flink expression
   * @return the tidb expression
   */
  private Optional<Expression> getExpression(ResolvedExpression resolvedExpression) {
    if (resolvedExpression instanceof CallExpression) {
      CallExpression callExpression = (CallExpression) resolvedExpression;
      FlinkExpression flinkExpression = FlinkExpression.from(callExpression);
      List<ResolvedExpression> resolvedChildren = callExpression.getResolvedChildren();
      if (COMPARISON_BINARY_FILTERS.contains(flinkExpression)) {
        return comparison(callExpression);
      }
      switch (flinkExpression) {
        case and:
          return and(resolvedChildren);
        case or:
          return or(resolvedChildren);
        case not:
          Optional<Expression> expression = getExpression(resolvedChildren.get(0));
          if (!expression.isPresent()) {
            return Optional.empty();
          }
          return emptyIfNotSupported(Expressions.not(expression.get()));
        case isNull:
          return isNull(((FieldReferenceExpression) resolvedChildren.get(0)).getName());
        case isNotNull:
          return isNotNull(((FieldReferenceExpression) resolvedChildren.get(0)).getName());
        default:
          return Optional.empty();
      }
    }
    return Optional.empty();
  }

  // We use empty as placeholder, which may cause the range of filters to become larger.
  // So we need to use the Result.of(Collections.emptyList(), filters) to filter again by flink.
  private Optional<Expression> emptyIfNotSupported(Expression expression) {
    return SupportedExpressionValidator.isSupportedExpression(expression, null)
        ? Optional.of(expression) : Optional.empty();
  }

  private Optional<Expression> isNull(String name) {
    return Optional.of(Expressions.isNull(Expressions.column(name, nameTypeMap.get(name))));
  }

  private Optional<Expression> isNotNull(String name) {
    return Optional.of(
        Expressions.not(Expressions.isNull(Expressions.column(name, nameTypeMap.get(name)))));
  }

  private Optional<ComparisonElement> column(FieldReferenceExpression expression,
      DataType castType) {
    ColumnElement element = new ColumnElement(expression.getName(), expression.getOutputDataType(),
        castType);
    return Optional.of(element);
  }

  private Optional<ComparisonElement> value(ValueLiteralExpression expression) {
    DataType dataType = expression.getOutputDataType();
    Object value = expression
        .getValueAs(dataType.getConversionClass())
        .orElseThrow(() -> new IllegalStateException("Can not get value"));
    return Optional.of(new ValueElement(value, dataType));
  }

  private Optional<ComparisonElement> convertSingleElement(final ResolvedExpression expression) {
    if (expression instanceof FieldReferenceExpression) {
      return column((FieldReferenceExpression) expression, null);
    }
    if (expression instanceof CallExpression
        && FlinkExpression.from((CallExpression) expression) == cast) {
      List<ResolvedExpression> children = expression.getResolvedChildren();
      return column((FieldReferenceExpression) children.get(0),
          children.get(1).getOutputDataType());
    }
    if (expression instanceof ValueLiteralExpression) {
      return value((ValueLiteralExpression) expression);
    }
    return Optional.empty();
  }

  private boolean isCastSupported(DataType type, DataType castType) {
    // Integer type is supported, because it will be auto converted to long.
    // Bytes to string and string to bytes may need to be supported.
    if (INTEGER_TYPES.contains(type.nullable()) && INTEGER_TYPES.contains(type.nullable())) {
      return true;
    }
    return type.getLogicalType().getDefaultConversion() == castType.getLogicalType()
        .getDefaultConversion();
  }

  private Optional<Expression> comparison(CallExpression callExpression) {
    List<ResolvedExpression> resolvedChildren = callExpression.getResolvedChildren();
    ComparisonElement leftElement = convertSingleElement(resolvedChildren.get(0)).orElse(null);
    if (leftElement == null) {
      return Optional.empty();
    }
    ComparisonElement rightElement = convertSingleElement(resolvedChildren.get(1)).orElse(null);
    if (rightElement == null) {
      return Optional.empty();
    }
    // Comparing value and value will be simplified in flink,
    // for example: '1 = 1' will be replace as null.
    // Comparing column and column is not supported now.
    // We only compare column and value.
    if (leftElement.getClass() == rightElement.getClass()) {
      return Optional.empty();
    }
    ValueElement valueElement =
        leftElement instanceof ValueElement ? (ValueElement) leftElement
            : (ValueElement) rightElement;
    ColumnElement columnElement =
        leftElement instanceof ColumnElement ? (ColumnElement) leftElement
            : (ColumnElement) rightElement;

    final String name = columnElement.getColumnName();
    final DataType type = columnElement.getType();
    final DataType castType = columnElement.getCastType();
    final org.tikv.common.types.DataType tidbType = nameTypeMap.get(name);
    final Object value = valueElement.getValue();
    final DataType valueType = valueElement.getType();

    // We will replace 'CAST(`c1` as int) = 1' with '`c1` = 1',
    // which means that 'CAST' syntax will be ignored, please use '`c1` = CAST(1 AS INT)'.
    if (castType != null && !isCastSupported(type, castType)) {
      return Optional.empty();
    }

    // Convert data
    Object resultValue = value;
    if (value instanceof LocalDate) {
      resultValue = Date.valueOf(((LocalDate) value));
    }
    if (value instanceof LocalDateTime) {
      resultValue = Timestamp.valueOf((LocalDateTime) value);
      if (tidbType.getType() == MySQLType.TypeTimestamp) {
        // to utc timestamp
        ZonedDateTime zonedDateTime = ZonedDateTime.of((LocalDateTime) value,
            ZoneId.systemDefault());
        ZonedDateTime utc = zonedDateTime.withZoneSameInstant(ZoneId.of("UTC"));
        resultValue = Timestamp.valueOf(utc.toLocalDateTime());
      }
    }

    // Convert Type, TODO: json and set
    org.tikv.common.types.DataType resultType = tidbType;
    if (tidbType.getType() == MySQLType.TypeEnum) {
      // If enum push down is not supported, we will return empty as a placeholder.
      if (!isSupportEnumPushDown() || !(value instanceof String)) {
        return Optional.empty();
      }
      resultType = StringType.VARCHAR;
    }

    Expression columnExpression = Expressions.column(name, resultType);
    Expression valueExpression = Expressions.constant(resultValue, resultType);

    Expression left = leftElement instanceof ValueElement ? valueExpression : columnExpression;
    Expression right = rightElement instanceof ValueElement ? valueExpression : columnExpression;

    switch (FlinkExpression.from(callExpression)) {
      case equals:
        return emptyIfNotSupported(Expressions.equal(left, right));
      case greaterThan:
        return emptyIfNotSupported(Expressions.greaterThan(left, right));
      case greaterThanOrEqual:
        return emptyIfNotSupported(Expressions.greaterEqual(left, right));
      case lessThan:
        return emptyIfNotSupported(Expressions.lessThan(left, right));
      case lessThanOrEqual:
        return emptyIfNotSupported(Expressions.lessEqual(left, right));
      case notEquals:
        return emptyIfNotSupported(Expressions.notEqual(left, right));
      case like:
        // Only `c1` like 'xxx' will be supported. That means c1 must be string type.
        if (leftElement instanceof ColumnElement
            && columnElement.getType().getConversionClass() == String.class) {
          return emptyIfNotSupported(Expressions.like(left, right));
        } else {
          return Optional.empty();
        }
      default:
        return Optional.empty();
    }
  }

  private Optional<Expression> and(List<ResolvedExpression> resolvedExpressions) {
    // Ignore all empty expressions.
    return Optional.ofNullable(Expressions.and(resolvedExpressions.stream()
        .map(this::getExpression)
        .filter(Optional::isPresent)
        .map(Optional::get)));
  }

  private Optional<Expression> or(List<ResolvedExpression> resolvedExpressions) {
    // If there is any empty in child, the or expression is null.
    List<Expression> expressions = resolvedExpressions.stream()
        .map(this::getExpression)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
    if (expressions.size() == 0 || expressions.size() < resolvedExpressions.size()) {
      return Optional.empty();
    }
    return Optional.ofNullable(Expressions.or(expressions));
  }

  public enum FlinkExpression {
    cast(BuiltInFunctionDefinitions.CAST),
    or(BuiltInFunctionDefinitions.OR),
    and(BuiltInFunctionDefinitions.AND),
    not(BuiltInFunctionDefinitions.NOT),
    greaterThan(BuiltInFunctionDefinitions.GREATER_THAN),
    greaterThanOrEqual(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL),
    lessThan(BuiltInFunctionDefinitions.LESS_THAN),
    lessThanOrEqual(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL),
    equals(BuiltInFunctionDefinitions.EQUALS),
    notEquals(BuiltInFunctionDefinitions.NOT_EQUALS),
    like(BuiltInFunctionDefinitions.LIKE),
    isNull(BuiltInFunctionDefinitions.IS_NULL),
    isNotNull(BuiltInFunctionDefinitions.IS_NOT_NULL),
    unresolved(null);

    private final FunctionDefinition functionDefinition;

    FlinkExpression(FunctionDefinition functionDefinition) {
      this.functionDefinition = functionDefinition;
    }

    public static FlinkExpression from(CallExpression callExpression) {
      return Arrays.stream(values())
          .filter(flinkExpression -> flinkExpression.functionDefinition
              == callExpression.getFunctionDefinition())
          .findFirst()
          .orElse(unresolved);
    }
  }

  interface ComparisonElement {

  }

  public static class ColumnElement implements ComparisonElement {

    private final String columnName;
    private final DataType type;
    private final DataType castType;


    protected ColumnElement(String columnName, DataType type, DataType castType) {
      this.columnName = columnName;
      this.type = type;
      this.castType = castType;
    }

    public String getColumnName() {
      return columnName;
    }

    public DataType getType() {
      return type;
    }

    public DataType getCastType() {
      return castType;
    }
  }

  public static class ValueElement implements ComparisonElement {

    private final Object value;
    private final DataType type;

    public ValueElement(Object value, DataType type) {
      this.value = value;
      this.type = type;
    }

    public Object getValue() {
      return value;
    }

    public DataType getType() {
      return type;
    }
  }

}