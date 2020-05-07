package com.amazon.opendistroforelasticsearch.sql.planner.physical;

import com.amazon.opendistroforelasticsearch.sql.data.model.ExprCollectionValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.expression.Expression;
import com.amazon.opendistroforelasticsearch.sql.expression.env.Environment;
import com.amazon.opendistroforelasticsearch.sql.storage.BindingTuple;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
public class AggregationOperator extends PhysicalPlan {
    private final PhysicalPlan input;
    private final List<Expression> aggExprs;
    private final List<Expression> groupByList;
    private Iterator<BindingTuple> iterator;

    @Override
    public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
        return visitor.visitAggregation(this, context);
    }

    @Override
    public List<PhysicalPlan> getChild() {
        return null;
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void open() {
        // populate from input
        List<BindingTuple> inputBindingTuples = new ArrayList<>();
        while (input.hasNext()) {
            inputBindingTuples.add(input.next());
        }
        if (inputBindingTuples.size() == 0) {
            return;
        }

        Group group = new Group(groupByList);

        // apply group by
        for (BindingTuple tuple : inputBindingTuples) {
            group.push(tuple);
        }
        // apply aggregation
        List<BindingTuple> bindingTupleResults = group.apply(aggExprs);
        iterator = bindingTupleResults.iterator();
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public BindingTuple next() {
        return iterator.next();
    }


    @RequiredArgsConstructor
    static class Group {
        private final List<Expression> groupExprs;
        private final Map<GroupKey, List<BindingTuple>> groupListMap = new HashMap<>();

        public void push(BindingTuple bindingTuple) {
            GroupKey groupKey = new GroupKey(groupExprs, bindingTuple);
            groupListMap.computeIfPresent(groupKey, (k, v) -> {
                v.add(bindingTuple);
                return v;
            });
            groupListMap.computeIfAbsent(groupKey, k -> {
                ArrayList<BindingTuple> tuples = new ArrayList<>();
                tuples.add(bindingTuple);
                return tuples;
            });
        }

        public List<BindingTuple> apply(List<Expression> aggExprs) {
            ImmutableList.Builder<BindingTuple> builder = new ImmutableList.Builder<>();
            for (Map.Entry<GroupKey, List<BindingTuple>> entry : groupListMap.entrySet()) {
                ImmutableMap.Builder<String, ExprValue> mapBuilder = new ImmutableMap.Builder();
                for (Expression aggregation : aggExprs) {
                    ExprValue exprValue = aggregation.valueOf(new Environment<Expression, ExprValue>() {
                        @Override
                        public ExprValue resolve(Expression expression) {
                            List<ExprValue> values = new ArrayList<>();
                            for (BindingTuple bindingTuple : entry.getValue()) {
                                values.add(expression.valueOf(bindingTuple));
                            }
                            return new ExprCollectionValue(values);
                        }
                    });
                    mapBuilder.putAll(entry.getKey().groupKeyMap());
                    mapBuilder.put(aggregation.toString(), exprValue);
                }
                builder.add(new BindingTuple(mapBuilder.build()));
            }
            return builder.build();
        }
    }

    static class GroupKey {
        private final List<Expression> groupExprs;;
        private final List<ExprValue> value;

        public GroupKey(List<Expression> groupExprs, BindingTuple bindingTuple) {
            this.groupExprs = groupExprs;
            this.value = new ArrayList<>();
            for (Expression groupExpr : groupExprs) {
                this.value.add(groupExpr.valueOf(bindingTuple));
            }
        }

        public Map<String, ExprValue> groupKeyMap() {
            Map<String, ExprValue> map = new HashMap<>();
            for (int i = 0; i < groupExprs.size(); i++) {
                map.put(groupExprs.get(i).toString(), value.get(i));
            }
            return map;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GroupKey groupKey = (GroupKey) o;

            return value.equals(groupKey.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }
}
