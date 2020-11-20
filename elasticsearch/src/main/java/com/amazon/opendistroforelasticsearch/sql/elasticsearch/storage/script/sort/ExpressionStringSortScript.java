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

import com.amazon.opendistroforelasticsearch.sql.data.model.ExprNullValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.script.core.ExpressionScript;
import com.amazon.opendistroforelasticsearch.sql.expression.Expression;
import com.amazon.opendistroforelasticsearch.sql.expression.env.Environment;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.script.StringSortScript;
import org.elasticsearch.search.lookup.SearchLookup;

/**
 * Todo.
 */
public class ExpressionStringSortScript extends StringSortScript {

  /**
   * Expression Script.
   */
  private final ExpressionScript expressionScript;

  public ExpressionStringSortScript(Expression expression,
                                    SearchLookup lookup,
                                    LeafReaderContext context,
                                    Map<String, Object> params) {
    super(params, lookup, context);
    this.expressionScript = new ExpressionScript(expression);
  }

  @Override
  public String execute() {
    return expressionScript.execute(this::getDoc, this::evaluateExpression).stringValue();
  }

  private ExprValue evaluateExpression(Expression expression,
                                       Environment<Expression, ExprValue> valueEnv) {
    ExprValue result = expression.valueOf(valueEnv);
    if (result.isNull()) {
      return ExprNullValue.of();
    }

    if (result.type() != ExprCoreType.STRING) {
      throw new IllegalStateException(String.format(
          "Expression has wrong result type instead of string: "
              + "expression [%s], result [%s]", expression, result));
    }
    return result;
  }
}

