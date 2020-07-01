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

package com.amazon.opendistroforelasticsearch.sql.newexpression;

import com.amazon.opendistroforelasticsearch.sql.expression.env.Environment;
import com.amazon.opendistroforelasticsearch.sql.newexpression.type.NExprType;
import com.amazon.opendistroforelasticsearch.sql.newexpression.type.NIntExprType;
import com.amazon.opendistroforelasticsearch.sql.newexpression.value.NExprValue;
import com.amazon.opendistroforelasticsearch.sql.newexpression.value.NExprValueFactory;

public class NExpressionFactory {

  public static NExpression intEqual(NExpression expr1, NExpression expr2) {
    return new NExpression() {
      @Override
      public NExprValue valueOf(Environment<NExpression, NExprValue> env) {
        return NExprValueFactory.intEqual(expr1.valueOf(env), expr2.valueOf(env));
      }

      @Override
      public NExprType type() {
        return NIntExprType.INT_TYPE;
      }
    };
  }

  public static NExpression path(NExpression source, NExpression ref) {
    return new NExpression() {
      @Override
      public NExprValue valueOf(Environment<NExpression, NExprValue> valueEnv) {
        NExprValue sourceValue = source.valueOf(valueEnv);
        return ref.valueOf(sourceValue.bindingTuples());
      }

      @Override
      public NExprType type() {
        return ref.type();
      }
    };
  }

  public static NExpression value(NExprValue value) {
    return new NExpression() {
      @Override
      public NExprValue valueOf(Environment<NExpression, NExprValue> valueEnv) {
        return value;
      }

      @Override
      public NExprType type() {
        return value.type();
      }
    };
  }

  public static NExpression ref(String ident, NExprType type) {
    return new NRefExpression(ident, type);
  }
}
