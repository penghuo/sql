package com.amazon.opendistroforelasticsearch.sql.planner.physical;

import com.amazon.opendistroforelasticsearch.sql.expression.DSL;
import com.amazon.opendistroforelasticsearch.sql.expression.ExpressionTestBase;
import com.amazon.opendistroforelasticsearch.sql.expression.aggregation.AvgAggregation;
import com.amazon.opendistroforelasticsearch.sql.storage.BindingTuple;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AggregationOperatorTest extends ExpressionTestBase {
    @Test
    public void aggTest() {
        PhysicalPlan plan = new AggregationOperator(new TestScan(),
                Arrays.asList(new AvgAggregation(DSL.ref("double_value"))),
                Arrays.asList(DSL.ref("integer_value")));
        List<BindingTuple> result = new ArrayList<>();

        plan.open();
        while (plan.hasNext()) {
            result.add(plan.next());
        }

        assertEquals(2, result.size());
    }

    private class TestScan extends PhysicalPlan {
        List<BindingTuple> tuples = Arrays.asList(
                BindingTuple.from(ImmutableMap.of("integer_value", 31, "string_value", "m", "double_value", 3d)),
                BindingTuple.from(ImmutableMap.of("integer_value", 31, "string_value", "f", "double_value", 4d)),
                BindingTuple.from(ImmutableMap.of("integer_value", 39, "string_value", "m", "double_value", 5d)),
                BindingTuple.from(ImmutableMap.of("integer_value", 39, "string_value", "f", "double_value", 3d)));
        Iterator<BindingTuple> iterator;


        public TestScan() {
            iterator = tuples.iterator();
        }

        @Override
        public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
            return null;
        }

        @Override
        public List<PhysicalPlan> getChild() {
            return null;
        }

        @Override
        public void close() throws Exception {

        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public BindingTuple next() {
            return iterator.next();
        }
    }

}