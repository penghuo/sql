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

package com.amazon.opendistroforelasticsearch.sql.expression;

import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;
import com.amazon.opendistroforelasticsearch.sql.expression.env.Environment;

/**
 * Path Reference Expression which represent a path to access tuple value.
 * e.g. the path is "employee.address.city", which will been compiled as PathReferenceExpression.
 * PathReferenceExpression("employee") (next)--->
 *  PathReferenceExpression("address") (next)--->
 *    ReferenceExpression("city)
 */
public class PathReferenceExpression extends ReferenceExpression {
  private final ReferenceExpression next;

  public PathReferenceExpression(String attr,
                                 ExprType type,
                                 ReferenceExpression next) {
    super(attr, type);
    this.next = next;
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    return next.valueOf(super.valueOf(valueEnv).bindingTuples());
  }

  @Override
  public ExprType type() {
    return next.type();
  }

  @Override
  public String toString() {
    return String.format("%s.%s", super.toString(), next.toString());
  }
}
