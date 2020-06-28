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
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class NRefExpression implements NExpression {
  private final String attr;
  private final NExprType exprType;

  @Override
  public NExprValue valueOf(Environment<NExpression, NExprValue> valueEnv) {
    return valueEnv.resolve(this);
  }

  @Override
  public NExprType type() {
    return exprType;
  }

  public NExprValue valueOf(JsonNode jsonNode) {
    JsonNode node = jsonNode.at(String.format("/%s", attr));
    return exprType.ofJson(node);
  }
}
