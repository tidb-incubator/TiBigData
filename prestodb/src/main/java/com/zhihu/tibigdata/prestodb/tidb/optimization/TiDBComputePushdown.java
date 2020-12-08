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

import static com.facebook.presto.expressions.translator.FunctionTranslator.buildFunctionTranslator;
import static com.facebook.presto.expressions.translator.RowExpressionTreeTranslator.translateWith;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static java.util.Objects.requireNonNull;

import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.expressions.translator.TranslatedExpression;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.PlanVisitor;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.collect.ImmutableList;
import com.zhihu.tibigdata.prestodb.tidb.TiDBSession;
import com.zhihu.tibigdata.prestodb.tidb.TiDBTableHandle;
import com.zhihu.tibigdata.prestodb.tidb.TiDBTableLayoutHandle;
import com.zhihu.tibigdata.tidb.Expressions;
import java.util.Optional;
import java.util.Set;
import org.tikv.common.expression.Expression;

public final class TiDBComputePushdown implements ConnectorPlanOptimizer {

  private final TiDBSession session;
  private final ExpressionOptimizer expressionOptimizer;
  private final PredicateTranslator predicateTranslator;
  private final LogicalRowExpressions logicalRowExpressions;

  public TiDBComputePushdown(
      TiDBSession session,
      FunctionMetadataManager functionMetadataManager,
      StandardFunctionResolution functionResolution,
      DeterminismEvaluator determinismEvaluator,
      ExpressionOptimizer expressionOptimizer,
      Set<Class<?>> functionTranslators) {
    requireNonNull(functionMetadataManager, "functionMetadataManager is null");
    requireNonNull(functionTranslators, "functionTranslators is null");
    requireNonNull(determinismEvaluator, "determinismEvaluator is null");
    requireNonNull(functionResolution, "functionResolution is null");
    this.session = requireNonNull(session, "session is null");
    this.expressionOptimizer = requireNonNull(expressionOptimizer, "expressionOptimizer is null");
    this.predicateTranslator = new PredicateTranslator(
        functionMetadataManager,
        buildFunctionTranslator(functionTranslators));
    this.logicalRowExpressions = new LogicalRowExpressions(
        determinismEvaluator,
        functionResolution,
        functionMetadataManager);
  }

  @Override
  public PlanNode optimize(
      PlanNode maxSubplan,
      ConnectorSession session,
      VariableAllocator variableAllocator,
      PlanNodeIdAllocator idAllocator) {
    return maxSubplan.accept(new Visitor(session, idAllocator), null);
  }

  private class Visitor
      extends PlanVisitor<PlanNode, Void> {

    private final ConnectorSession session;
    private final PlanNodeIdAllocator idAllocator;

    public Visitor(ConnectorSession session, PlanNodeIdAllocator idAllocator) {
      this.session = requireNonNull(session, "session is null");
      this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
    }

    @Override
    public PlanNode visitPlan(PlanNode node, Void context) {
      ImmutableList.Builder<PlanNode> children = ImmutableList.builder();
      boolean changed = false;
      for (PlanNode child : node.getSources()) {
        PlanNode newChild = child.accept(this, null);
        if (newChild != child) {
          changed = true;
        }
        children.add(newChild);
      }

      if (!changed) {
        return node;
      }
      return node.replaceChildren(children.build());
    }

    @Override
    public PlanNode visitFilter(FilterNode node, Void context) {
      if (!(node.getSource() instanceof TableScanNode)) {
        return node;
      }

      TableScanNode oldTableScanNode = (TableScanNode) node.getSource();
      TableHandle oldTableHandle = oldTableScanNode.getTable();
      RowExpression predicate = expressionOptimizer
          .optimize(node.getPredicate(), OPTIMIZED, session);
      predicate = logicalRowExpressions.convertToConjunctiveNormalForm(predicate);

      TranslatedExpression<Expression> translatedExpression = translateWith(predicate,
          predicateTranslator, oldTableScanNode.getAssignments());

      TiDBTableHandle oldConnectorTable = (TiDBTableHandle) oldTableHandle.getConnectorHandle();

      Optional<Expression> translated = translatedExpression.getTranslated();
      Optional<Expression> tupleDomain = oldTableHandle.getLayout().flatMap(layout -> {
        TiDBTableLayoutHandle tidbLayout = (TiDBTableLayoutHandle) layout;
        return TupleDomainTranslator.translate(TiDBComputePushdown.this.session, oldConnectorTable,
            tidbLayout.getTupleDomain().get());
      });

      Optional<Expression> additionalPredicate = Expressions.and(tupleDomain, translated);

      if (!additionalPredicate.isPresent()) {
        return node;
      }

      TiDBTableLayoutHandle newTableLayoutHandle = new TiDBTableLayoutHandle(
          oldConnectorTable,
          Optional.empty(),
          additionalPredicate.map(Expressions::serialize));

      TableHandle tableHandle = new TableHandle(
          oldTableHandle.getConnectorId(),
          oldTableHandle.getConnectorHandle(),
          oldTableHandle.getTransaction(),
          Optional.of(newTableLayoutHandle));

      TableScanNode newTableScanNode = new TableScanNode(
          idAllocator.getNextId(),
          tableHandle,
          oldTableScanNode.getOutputVariables(),
          oldTableScanNode.getAssignments(),
          oldTableScanNode.getCurrentConstraint(),
          oldTableScanNode.getEnforcedConstraint());

      return new FilterNode(idAllocator.getNextId(), newTableScanNode, node.getPredicate());
    }
  }
}
