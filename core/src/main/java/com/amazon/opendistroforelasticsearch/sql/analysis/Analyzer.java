/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.sql.analysis;

import com.amazon.opendistroforelasticsearch.sql.ast.AbstractNodeVisitor;
import com.amazon.opendistroforelasticsearch.sql.ast.expression.UnresolvedExpression;
import com.amazon.opendistroforelasticsearch.sql.ast.tree.Aggregation;
import com.amazon.opendistroforelasticsearch.sql.ast.tree.Filter;
import com.amazon.opendistroforelasticsearch.sql.ast.tree.Join;
import com.amazon.opendistroforelasticsearch.sql.ast.tree.Relation;
import com.amazon.opendistroforelasticsearch.sql.ast.tree.UnresolvedPlan;
import com.amazon.opendistroforelasticsearch.sql.expression.DSL;
import com.amazon.opendistroforelasticsearch.sql.expression.Expression;
import com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalAggregation;
import com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalFilter;
import com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalJoin;
import com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalPlan;
import com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalRelation;
import com.amazon.opendistroforelasticsearch.sql.storage.StorageEngine;
import com.amazon.opendistroforelasticsearch.sql.storage.Table;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Analyze the {@link UnresolvedPlan} in the {@link AnalysisContext} to construct the {@link LogicalPlan}
 */
@RequiredArgsConstructor
public class Analyzer extends AbstractNodeVisitor<LogicalPlan, AnalysisContext> {
    private final ExpressionAnalyzer expressionAnalyzer;
    private final StorageEngine storageEngine;

    public LogicalPlan analyze(UnresolvedPlan unresolved, AnalysisContext context) {
        return unresolved.accept(this, context);
    }

    @Override
    public LogicalPlan visitRelation(Relation node, AnalysisContext context) {
        context.push();
        TypeEnvironment curEnv = context.peek();
        Table table = storageEngine.getTable(node.getTableName());
        table.getFieldTypes().forEach((k, v) -> curEnv.define(DSL.ref(k), v));
        return new LogicalRelation(node.getTableName());
    }

    @Override
    public LogicalPlan visitFilter(Filter node, AnalysisContext context) {
        LogicalPlan child = node.getChild().get(0).accept(this, context);
        Expression condition = expressionAnalyzer.analyze(node.getCondition(), context);
        return new LogicalFilter(condition, child);
    }

    @Override
    public LogicalPlan visitJoin(Join node, AnalysisContext context) {
        return new LogicalJoin(
                node.getChildren().get(0).accept(this, context),
                node.getChildren().get(1).accept(this, context),
                node.getJoinType(),
                node.getJoinFieldNames()
        );
    }

    @Override
    public LogicalPlan visitAggregation(Aggregation node, AnalysisContext context) {
        return new LogicalAggregation(
                node.getChild().get(0).accept(this, context),
                visitExpressions(node.getAggExprList(), context),
                visitExpressions(node.getGroupExprList(), context)
        );
    }

    List<Expression> visitExpressions(List<UnresolvedExpression> expressions, AnalysisContext context) {
        return expressions.stream().map(expr -> expressionAnalyzer.analyze(expr, context)).collect(Collectors.toList());
    }
}
