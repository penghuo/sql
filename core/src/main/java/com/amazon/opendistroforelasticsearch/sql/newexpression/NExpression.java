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
import com.amazon.opendistroforelasticsearch.sql.newexpression.value.NExprValue;

public interface NExpression {

  /**
   * Evaluate the value of expression in the value environment.
   */
  NExprValue valueOf(Environment<NExpression, NExprValue> valueEnv);

  /**
   * Evaluate the type of expression in the type environment.
   */
  NExprType type();
}
