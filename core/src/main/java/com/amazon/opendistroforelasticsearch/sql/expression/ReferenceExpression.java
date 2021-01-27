/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.sql.expression;

import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;
import com.amazon.opendistroforelasticsearch.sql.expression.env.Environment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@EqualsAndHashCode
public class ReferenceExpression implements Expression {

  @Getter
  private final List<String> paths;

  private final ExprType type;

  /**
   * Todo.
   */
  public ReferenceExpression(String path,
                             ExprType type) {
    this.paths = Collections.singletonList(path);
    this.type = type;
  }

  /**
   * Todo.
   */
  public ReferenceExpression(String bindName,
                             List<String> paths,
                             ExprType type) {
    this.paths = new ArrayList<>();
    this.paths.add(bindName);
    this.paths.addAll(paths);
    this.type = type;
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> env) {
    return env.resolve(this);
  }

  @Override
  public ExprType type() {
    return type;
  }

  @Override
  public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
    return visitor.visitReference(this, context);
  }

  @Override
  public String toString() {
    return String.join(".", paths);
  }

  public String getAttr() {
    return String.join(".", paths);
  }

  /**
   * Todo.
   */
  public ExprValue resolve(Function<String, ExprValue> bindings) {
    if (paths.size() == 1) {
      return bindings.apply(paths.get(0));
    } else {
      return bindings.apply(paths.get(0)).pathValue(paths.subList(1, paths.size()));
    }
  }
}
