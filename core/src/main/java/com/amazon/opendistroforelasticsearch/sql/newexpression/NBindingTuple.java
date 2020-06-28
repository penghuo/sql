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
import com.amazon.opendistroforelasticsearch.sql.newexpression.value.NExprValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;

public class NBindingTuple implements Environment<NExpression, NExprValue> {
  private static ObjectMapper mapper = new ObjectMapper();
  private final JsonNode jsonNode;

  @SneakyThrows
  public NBindingTuple(String json) {
    this.jsonNode = mapper.readTree(json);
  }

  @Override
  public NExprValue resolve(NExpression expr) {
    if (expr instanceof NRefExpression) {
      return ((NRefExpression) expr).valueOf(jsonNode);
    } else {
      throw new IllegalStateException("only ref expression could been resolved");
    }
  }
}
