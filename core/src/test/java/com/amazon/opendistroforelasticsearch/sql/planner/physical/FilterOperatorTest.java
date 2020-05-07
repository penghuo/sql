package com.amazon.opendistroforelasticsearch.sql.planner.physical;

import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValueUtils;
import com.amazon.opendistroforelasticsearch.sql.expression.DSL;
import com.amazon.opendistroforelasticsearch.sql.expression.ExpressionTestBase;
import com.amazon.opendistroforelasticsearch.sql.storage.BindingTuple;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FilterOperatorTest extends ExpressionTestBase {

    @Test
    public void filterTest() {
        FilterOperator command = new FilterOperator(new TestScan(),
                dsl.equal(typeEnv(), DSL.ref("integer_value"), DSL.literal(ExprValueUtils.integerValue(31))));
        List<BindingTuple> result = new ArrayList<>();
        while (command.hasNext()) {
            result.add(command.next());
        }
        assertEquals(2, result.size());
    }

    private class TestScan extends PhysicalPlan {
        List<BindingTuple> tuples = Arrays.asList(
                BindingTuple.from(ImmutableMap.of("integer_value", 31, "string_value", "m")),
                BindingTuple.from(ImmutableMap.of("integer_value", 31, "string_value", "f")),
                BindingTuple.from(ImmutableMap.of("integer_value", 39, "string_value", "m")),
                BindingTuple.from(ImmutableMap.of("integer_value", 39, "string_value", "f")));
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