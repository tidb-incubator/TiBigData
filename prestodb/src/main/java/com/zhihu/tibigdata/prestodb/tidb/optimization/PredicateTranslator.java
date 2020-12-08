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

package com.zhihu.tibigdata.prestodb.tidb.optimization;

import static com.facebook.presto.expressions.translator.TranslatedExpression.untranslated;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.zhihu.tibigdata.prestodb.tidb.optimization.TiDBPlanOptimizerProvider.isPushdownType;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.tikv.common.types.DateType.DATE;
import static org.tikv.common.types.IntegerType.BIGINT;
import static org.tikv.common.types.IntegerType.INT;
import static org.tikv.common.types.IntegerType.SMALLINT;
import static org.tikv.common.types.IntegerType.TINYINT;
import static org.tikv.common.types.RealType.DOUBLE;
import static org.tikv.common.types.RealType.FLOAT;
import static org.tikv.common.types.StringType.CHAR;
import static org.tikv.common.types.StringType.VARCHAR;
import static org.tikv.common.types.TimeType.TIME;
import static org.tikv.common.types.TimestampType.TIMESTAMP;

import com.facebook.presto.expressions.translator.FunctionTranslator;
import com.facebook.presto.expressions.translator.RowExpressionTranslator;
import com.facebook.presto.expressions.translator.RowExpressionTreeTranslator;
import com.facebook.presto.expressions.translator.TranslatedExpression;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.zhihu.tibigdata.prestodb.tidb.TiDBColumnHandle;
import com.zhihu.tibigdata.tidb.Expressions;
import io.airlift.slice.Slice;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.tikv.common.expression.Expression;

public final class PredicateTranslator extends
    RowExpressionTranslator<Expression, Map<VariableReferenceExpression, ColumnHandle>> {

  private final FunctionMetadataManager functionMetadataManager;
  private final FunctionTranslator<Expression> functionTranslator;

  public PredicateTranslator(FunctionMetadataManager functionMetadataManager,
      FunctionTranslator<Expression> functionTranslator) {
    this.functionMetadataManager = requireNonNull(functionMetadataManager,
        "functionMetadataManager is null");
    this.functionTranslator = requireNonNull(functionTranslator, "functionTranslator is null");
  }

  public static Expression tryConvert(ConstantExpression literal) {
    Type type = literal.getType();
    Expression exp = null;
    if (isPushdownType(type)) {
      Object value = literal.getValue();
      if (value instanceof Slice) {
        value = ((Slice) value).toStringUtf8();
      }
      if (type.equals(BigintType.BIGINT)) {
        exp = Expressions.constant(value, BIGINT);
      } else if (type.equals(TinyintType.TINYINT)) {
        exp = Expressions.constant(value, TINYINT);
      } else if (type.equals(SmallintType.SMALLINT)) {
        exp = Expressions.constant(value, SMALLINT);
      } else if (type.equals(IntegerType.INTEGER)) {
        exp = Expressions.constant(value, INT);
      } else if (type.equals(DoubleType.DOUBLE)) {
        exp = Expressions.constant(value, DOUBLE);
      } else if (type.equals(RealType.REAL)) {
        exp = Expressions.constant(value, FLOAT);
      } else if (type.equals(BooleanType.BOOLEAN)) {
        exp = Expressions.constant(value, TINYINT);
      } else if (type.equals(DateType.DATE)) {
        exp = Expressions.constant(value, DATE);
      } else if (type.equals(TimeType.TIME)) {
        exp = Expressions.constant(value, TIME);
      } else if (type.equals(TimestampType.TIMESTAMP)) {
        exp = Expressions.constant(value, TIMESTAMP);
      } else if (type instanceof VarcharType) {
        exp = Expressions.constant(value, VARCHAR);
      } else if (type instanceof CharType) {
        exp = Expressions.constant(value, CHAR);
      }
    }
    return exp;
  }

  @Override
  public TranslatedExpression<Expression> translateConstant(ConstantExpression literal,
      Map<VariableReferenceExpression, ColumnHandle> context,
      RowExpressionTreeTranslator<Expression, Map<VariableReferenceExpression,
          ColumnHandle>> rowExpressionTreeTranslator) {
    Expression exp = tryConvert(literal);
    if (exp != null) {
      return new TranslatedExpression<>(Optional.of(exp), literal, ImmutableList.of());
    } else {
      return untranslated(literal);
    }
  }

  @Override
  public TranslatedExpression<Expression> translateVariable(VariableReferenceExpression variable,
      Map<VariableReferenceExpression, ColumnHandle> context,
      RowExpressionTreeTranslator<Expression,
          Map<VariableReferenceExpression, ColumnHandle>> rowExpressionTreeTranslator) {
    TiDBColumnHandle columnHandle = (TiDBColumnHandle) context.get(variable);
    requireNonNull(columnHandle, format("Unrecognized variable %s", variable));
    return new TranslatedExpression<>(
        Optional.of(Expressions.column(columnHandle.getName())),
        variable,
        ImmutableList.of());
  }

  @Override
  public TranslatedExpression<Expression> translateLambda(LambdaDefinitionExpression lambda,
      Map<VariableReferenceExpression, ColumnHandle> context,
      RowExpressionTreeTranslator<Expression,
          Map<VariableReferenceExpression, ColumnHandle>> rowExpressionTreeTranslator) {
    return untranslated(lambda);
  }

  @Override
  public TranslatedExpression<Expression> translateCall(CallExpression call,
      Map<VariableReferenceExpression, ColumnHandle> context,
      RowExpressionTreeTranslator<Expression,
          Map<VariableReferenceExpression, ColumnHandle>> rowExpressionTreeTranslator) {
    List<TranslatedExpression<Expression>> translatedExpressions = call.getArguments().stream()
        .map(expression -> rowExpressionTreeTranslator.rewrite(expression, context))
        .collect(toImmutableList());

    FunctionMetadata functionMetadata = functionMetadataManager
        .getFunctionMetadata(call.getFunctionHandle());

    try {
      return functionTranslator.translate(functionMetadata, call, translatedExpressions);
    } catch (Throwable t) {
      // no-op
    }
    return untranslated(call, translatedExpressions);
  }

  @Override
  public TranslatedExpression<Expression> translateSpecialForm(SpecialFormExpression specialForm,
      Map<VariableReferenceExpression, ColumnHandle> context,
      RowExpressionTreeTranslator<Expression,
          Map<VariableReferenceExpression, ColumnHandle>> rowExpressionTreeTranslator) {
    List<TranslatedExpression<Expression>> translatedExpressions = specialForm.getArguments()
        .stream()
        .map(expression -> rowExpressionTreeTranslator.rewrite(expression, context))
        .collect(toImmutableList());

    List<Expression> expressions = translatedExpressions.stream()
        .map(TranslatedExpression::getTranslated)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(toImmutableList());

    if (expressions.size() < translatedExpressions.size()) {
      return untranslated(specialForm, translatedExpressions);
    }

    switch (specialForm.getForm()) {
      case AND:
        return new TranslatedExpression<>(
            Optional.of(Expressions.and(expressions)),
            specialForm,
            translatedExpressions);
      case OR:
        return new TranslatedExpression<>(
            Optional.of(Expressions.or(expressions)),
            specialForm,
            translatedExpressions);
      case IN:
        return new TranslatedExpression<>(
            Optional.of(Expressions.in(expressions)),
            specialForm,
            translatedExpressions);
      default:
        break;
    }
    return untranslated(specialForm, translatedExpressions);
  }
}