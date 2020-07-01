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

package com.amazon.opendistroforelasticsearch.sql.newexpression.bindings;

import com.amazon.opendistroforelasticsearch.sql.exception.ExpressionEvaluationException;
import com.amazon.opendistroforelasticsearch.sql.expression.Expression;
import com.amazon.opendistroforelasticsearch.sql.expression.ReferenceExpression;
import com.amazon.opendistroforelasticsearch.sql.expression.env.Environment;
import com.amazon.opendistroforelasticsearch.sql.newexpression.NExpression;
import com.amazon.opendistroforelasticsearch.sql.newexpression.NRefExpression;
import com.amazon.opendistroforelasticsearch.sql.newexpression.value.NExprValue;

public abstract class NBindingTuple implements Environment<NExpression, NExprValue> {
  /**
   * Resolve {@link Expression} in the BindingTuple environment.
   */
  @Override
  public NExprValue resolve(NExpression var) {
    if (var instanceof NRefExpression) {
      return resolve(((NRefExpression) var));
    } else {
      throw new ExpressionEvaluationException(String.format("can resolve expression: %s", var));
    }
  }

  /**
   * Resolve the {@link ReferenceExpression} in BindingTuple context.
   */
  public abstract NExprValue resolve(NRefExpression ref);
}
