/*
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.script.sort;

import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.BOOLEAN;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.BYTE;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.DATE;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.DATETIME;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.DOUBLE;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.FLOAT;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.INTEGER;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.LONG;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.SHORT;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.TIME;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.TIMESTAMP;

import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.script.core.ExpressionScript;
import com.amazon.opendistroforelasticsearch.sql.expression.Expression;
import com.amazon.opendistroforelasticsearch.sql.expression.env.Environment;
import java.util.Map;
import lombok.EqualsAndHashCode;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.script.NumberSortScript;
import org.elasticsearch.search.lookup.SearchLookup;

/**
 * Expression script executor that executes the expression on each document
 * and determine if the document is supposed to be filtered out or not.
 */
@EqualsAndHashCode(callSuper = false)
public class ExpressionNumberSortScript extends NumberSortScript {

  /**
   * Expression Script.
   */
  private final ExpressionScript expressionScript;

  public ExpressionNumberSortScript(Expression expression,
                                    SearchLookup lookup,
                                    LeafReaderContext context,
                                    Map<String, Object> params) {
    super(params, lookup, context);
    this.expressionScript = new ExpressionScript(expression);
  }

  @Override
  public double execute() {
    final ExprValue value = expressionScript.execute(this::getDoc, this::evaluateExpression);
    if (value.isNull()) {
      return 0;
    } else {
      ExprType type = value.type();
      if (BYTE.equals(type) || SHORT.equals(type) || INTEGER.equals(type) || LONG.equals(type)
          || FLOAT.equals(type) || DOUBLE.equals(type)) {
        return value.doubleValue();
      } else if (TIMESTAMP.equals(type) || DATE.equals(type) || DATETIME.equals(type)) {
        return value.timestampValue().toEpochMilli();
      } else if (TIME.equals(type)) {
        return value.timeValue().toSecondOfDay();
      } else if (BOOLEAN.equals(type)) {
        return value.booleanValue() ? 1d : 0d;
      }
      throw new IllegalStateException("Unexpected value type: " + value.type());
    }
  }

  private ExprValue evaluateExpression(Expression expression,
                                       Environment<Expression, ExprValue> valueEnv) {
    return expression.valueOf(valueEnv);
  }

}
