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

package com.amazon.opendistroforelasticsearch.sql.legacy.query.planner.physical.node.project;

import com.amazon.opendistroforelasticsearch.sql.legacy.expression.domain.BindingTuple;
import com.amazon.opendistroforelasticsearch.sql.legacy.expression.model.ExprMissingValue;
import com.amazon.opendistroforelasticsearch.sql.legacy.expression.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.legacy.plugin.RestSqlAction;
import com.amazon.opendistroforelasticsearch.sql.legacy.query.planner.core.ColumnNode;
import com.amazon.opendistroforelasticsearch.sql.legacy.query.planner.core.PlanNode;
import com.amazon.opendistroforelasticsearch.sql.legacy.query.planner.physical.PhysicalOperator;
import com.amazon.opendistroforelasticsearch.sql.legacy.query.planner.physical.Row;
import com.amazon.opendistroforelasticsearch.sql.legacy.query.planner.physical.estimation.Cost;
import com.amazon.opendistroforelasticsearch.sql.legacy.query.planner.physical.node.scroll.BindingTupleRow;
import lombok.RequiredArgsConstructor;

import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The definition of Project Operator.
 */
@RequiredArgsConstructor
public class PhysicalProject implements PhysicalOperator<BindingTuple> {

    private static final Logger LOG = LogManager.getLogger(PhysicalProject.class);

    private final PhysicalOperator<BindingTuple> next;
    private final List<ColumnNode> fields;

    @Override
    public Cost estimate() {
        return null;
    }

    @Override
    public PlanNode[] children() {
        return new PlanNode[]{next};
    }

    @Override
    public boolean hasNext() {
        return next.hasNext();
    }

    @Override
    public Row<BindingTuple> next() {
        BindingTuple input = next.next().data();
        BindingTuple.BindingTupleBuilder outputBindingTupleBuilder = BindingTuple.builder();
        fields.forEach(field -> {
            final ExprValue exprValue = field.getExpr().valueOf(input);
            if (exprValue instanceof ExprMissingValue) {
//                LOG.error(String.format("field:%s resolved as missing in context: %s", field, input));
                throw new RuntimeException(String.format("field:%s resolved as missing in "
                    + "context: %s", field, input));
            }
            outputBindingTupleBuilder.binding(field.getName(), exprValue);
        });
        return new BindingTupleRow(outputBindingTupleBuilder.build());
    }
}
