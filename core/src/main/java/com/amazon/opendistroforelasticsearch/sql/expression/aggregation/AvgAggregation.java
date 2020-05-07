package com.amazon.opendistroforelasticsearch.sql.expression.aggregation;

import com.amazon.opendistroforelasticsearch.sql.data.model.ExprNullValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprType;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValueUtils;
import com.amazon.opendistroforelasticsearch.sql.expression.Aggregation;
import com.amazon.opendistroforelasticsearch.sql.expression.Expression;
import com.amazon.opendistroforelasticsearch.sql.expression.env.Environment;
import lombok.RequiredArgsConstructor;

import java.util.List;

import static com.amazon.opendistroforelasticsearch.sql.data.model.ExprValueUtils.getDoubleValue;

@RequiredArgsConstructor
public class AvgAggregation extends Aggregation {
    private final Expression expression;

    @Override
    public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
        ExprValue exprValue = expression.valueOf(valueEnv);
        List<ExprValue> collectionValue = ExprValueUtils.getCollectionValue(exprValue);
        if (collectionValue.size() == 0) {
            return ExprNullValue.of();
        }
        ExprValue result = ExprValueUtils.doubleValue(0d);
        int count = 0;
        for (ExprValue value : collectionValue) {
            if (value.isMissing() || value.isNull()) {
                return ExprNullValue.of();
            } else {
                count ++;
                result = add(result, value);
            }
        }
        return ExprValueUtils.fromObjectValue(ExprValueUtils.getDoubleValue(result)/count);
    }

    @Override
    public ExprType type(Environment<Expression, ExprType> typeEnv) {
        return ExprType.DOUBLE;
    }

    private ExprValue add(ExprValue v1, ExprValue v2) {
        return ExprValueUtils.fromObjectValue(getDoubleValue(v1) + getDoubleValue(v2));
    }

    @Override
    public String toString() {
        return String.format("avg(%s)", expression.toString());
    }
}
