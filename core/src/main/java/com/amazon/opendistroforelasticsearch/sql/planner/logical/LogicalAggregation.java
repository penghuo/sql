package com.amazon.opendistroforelasticsearch.sql.planner.logical;

import com.amazon.opendistroforelasticsearch.sql.expression.Expression;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.Arrays;
import java.util.List;

@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
public class LogicalAggregation extends LogicalPlan {
    private final LogicalPlan child;
    @Getter
    private final List<Expression> aggExprs;
    @Getter
    private final List<Expression> groupExprs;

    @Override
    public List<LogicalPlan> getChild() {
        return Arrays.asList(child);
    }

    @Override
    public <R, C> R accept(AbstractPlanNodeVisitor<R, C> visitor, C context) {
        return visitor.visitAggregation(this, context);
    }
}
