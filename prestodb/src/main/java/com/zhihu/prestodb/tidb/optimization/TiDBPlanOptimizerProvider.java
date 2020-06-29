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

package com.zhihu.prestodb.tidb.optimization;

import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.RowExpressionService;
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
import com.google.common.collect.ImmutableSet;
import com.zhihu.prestodb.tidb.TiDBSession;
import java.util.Set;
import javax.inject.Inject;

public final class TiDBPlanOptimizerProvider implements ConnectorPlanOptimizerProvider {

  private static final Set<Class<?>> functionTranslators = ImmutableSet
      .of(OperatorTranslators.class);
  private final FunctionMetadataManager functionMetadataManager;
  private final StandardFunctionResolution standardFunctionResolution;
  private final RowExpressionService rowExpressionService;
  private TiDBSession session;

  @Inject
  public TiDBPlanOptimizerProvider(
      TiDBSession session,
      FunctionMetadataManager functionMetadataManager,
      StandardFunctionResolution standardFunctionResolution,
      RowExpressionService rowExpressionService) {
    this.session = session;
    this.functionMetadataManager = functionMetadataManager;
    this.standardFunctionResolution = standardFunctionResolution;
    this.rowExpressionService = rowExpressionService;
  }

  public static boolean isPushdownType(Type type) {
    return type.isOrderable() && (type.equals(BigintType.BIGINT)
        || type.equals(TinyintType.TINYINT)
        || type.equals(SmallintType.SMALLINT)
        || type.equals(IntegerType.INTEGER)
        || type.equals(DoubleType.DOUBLE)
        || type.equals(RealType.REAL)
        || type.equals(BooleanType.BOOLEAN)
        || type.equals(DateType.DATE)
        || type.equals(TimeType.TIME)
        || type.equals(TimestampType.TIMESTAMP)
        || type instanceof VarcharType
        || type instanceof CharType);
  }

  /**
   * The plan optimizers to be applied before having the notion of distribution.
   */
  public Set<ConnectorPlanOptimizer> getLogicalPlanOptimizers() {
    return ImmutableSet.of();
  }

  /**
   * The plan optimizers to be applied after having the notion of distribution. The plan will be
   * only executed on a single node.
   */
  public Set<ConnectorPlanOptimizer> getPhysicalPlanOptimizers() {
    return ImmutableSet.of(
        new TiDBComputePushdown(
            session,
            functionMetadataManager,
            standardFunctionResolution,
            rowExpressionService.getDeterminismEvaluator(),
            rowExpressionService.getExpressionOptimizer(),
            functionTranslators));
  }
}
