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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.zhihu.tibigdata.prestodb.tidb.optimization.TiDBPlanOptimizerProvider.isPushdownType;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.zhihu.tibigdata.prestodb.tidb.TiDBColumnHandle;
import com.zhihu.tibigdata.prestodb.tidb.TiDBSession;
import com.zhihu.tibigdata.prestodb.tidb.TiDBTableHandle;
import com.zhihu.tibigdata.tidb.Expressions;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.tikv.common.expression.Expression;

public final class TupleDomainTranslator {

  private static Expression buildConjunctions(TiDBColumnHandle column, Domain domain,
      Expression expression) {
    Expression columnRef = column.createColumnExpression();
    ValueSet values = domain.getValues();
    if (values.isNone()) {
      if (domain.isNullAllowed()) {
        return Expressions.and(Expressions.isNull(columnRef), expression);
      } else {
        return Expressions.alwaysFalse();
      }
    }
    if (values.isAll()) {
      if (!domain.isNullAllowed()) {
        return Expressions.and(Expressions.not(Expressions.isNull(columnRef)), expression);
      } else {
        return expression;
      }
    }
    Expression disjunctions = null;
    for (Range range : values.getRanges().getOrderedRanges()) {
      checkState(!range.isAll());
      if (range.isSingleValue()) {
        disjunctions = Expressions.or(Expressions
                .equal(columnRef, column.createConstantExpression(range.getLow().getValue())),
            disjunctions);
      } else {
        Marker low = range.getLow();
        Marker high = range.getHigh();
        Expression rangeConjunctions = null;
        if (!low.isLowerUnbounded()) {
          switch (low.getBound()) {
            case ABOVE:
              rangeConjunctions = Expressions.and(Expressions
                      .greaterThan(columnRef, column.createConstantExpression(low.getValue())),
                  rangeConjunctions);
              break;
            case EXACTLY:
              rangeConjunctions = Expressions.and(Expressions
                      .greaterEqual(columnRef, column.createConstantExpression(low.getValue())),
                  rangeConjunctions);
              break;
            case BELOW:
              throw new IllegalArgumentException("Low marker should never use BELOW bound");
            default:
              throw new AssertionError("Unhandled bound: " + low.getBound());
          }
        }
        if (!high.isUpperUnbounded()) {
          switch (high.getBound()) {
            case ABOVE:
              throw new IllegalArgumentException("High marker should never use ABOVE bound");
            case EXACTLY:
              rangeConjunctions = Expressions.and(Expressions
                      .lessEqual(columnRef, column.createConstantExpression(high.getValue())),
                  rangeConjunctions);
              break;
            case BELOW:
              rangeConjunctions = Expressions.and(
                  Expressions.lessThan(columnRef, column.createConstantExpression(high.getValue())),
                  rangeConjunctions);
              break;
            default:
              throw new AssertionError("Unhandled bound: " + high.getBound());
          }
        }
        disjunctions = Expressions.or(rangeConjunctions, disjunctions);
      }
    }
    return Expressions.and(disjunctions, expression);
  }

  private static Map<TiDBColumnHandle, Domain> translatableDomains(
      Map<ColumnHandle, Domain> domains) {
    return domains.entrySet().stream()
        .filter(e -> isPushdownType(((TiDBColumnHandle) e.getKey()).getPrestoType()))
        .collect(toImmutableMap(e -> (TiDBColumnHandle) e.getKey(), Map.Entry::getValue));
  }

  public static Optional<Expression> translate(TiDBSession session, TiDBTableHandle table,
      TupleDomain<ColumnHandle> tupleDomain) {
    return tupleDomain.getDomains().map(domains -> {
      Expression expression = null;
      Map<TiDBColumnHandle, Domain> translatable = translatableDomains(domains);
      List<String> translatableColumns = translatable.keySet().stream()
          .map(TiDBColumnHandle::getName).collect(toImmutableList());
      for (Map.Entry<TiDBColumnHandle, Domain> entry : translatable.entrySet()) {
        TiDBColumnHandle column = (TiDBColumnHandle) entry.getKey();
        Domain domain = entry.getValue();
        expression = buildConjunctions(column, domain, expression);
      }
      return expression;
    });
  }
}
