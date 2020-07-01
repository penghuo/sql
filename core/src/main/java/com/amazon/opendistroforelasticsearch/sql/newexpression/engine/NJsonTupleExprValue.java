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

package com.amazon.opendistroforelasticsearch.sql.newexpression.engine;

import static com.amazon.opendistroforelasticsearch.sql.newexpression.type.NTupleExprType.TUPLE_TYPE;

import com.amazon.opendistroforelasticsearch.sql.newexpression.NRefExpression;
import com.amazon.opendistroforelasticsearch.sql.newexpression.bindings.NBindingTuple;
import com.amazon.opendistroforelasticsearch.sql.newexpression.type.NBoolExprType;
import com.amazon.opendistroforelasticsearch.sql.newexpression.type.NExprType;
import com.amazon.opendistroforelasticsearch.sql.newexpression.type.NIntExprType;
import com.amazon.opendistroforelasticsearch.sql.newexpression.value.NBoolExprValue;
import com.amazon.opendistroforelasticsearch.sql.newexpression.value.NExprValue;
import com.amazon.opendistroforelasticsearch.sql.newexpression.value.NIntExprValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;

public class NJsonTupleExprValue implements NExprValue {
  private static ObjectMapper mapper = new ObjectMapper();
  private final JsonNode jsonNode;

  public NJsonTupleExprValue(JsonNode jsonNode) {
    this.jsonNode = jsonNode;
  }

  @SneakyThrows
  public NJsonTupleExprValue(String jsonString) {
    this.jsonNode = mapper.readTree(jsonString);
  }

  @Override
  public NExprType type() {
    return TUPLE_TYPE;
  }

  @Override
  public Object getValue() {
    return jsonNode;
  }

  @Override
  public NBindingTuple bindingTuples() {
    return new NBindingTuple() {
      @Override
      public NExprValue resolve(NRefExpression expr) {
        if (expr instanceof NRefExpression) {
          JsonNode node = jsonNode.at(String.format("/%s", ((NRefExpression) expr).getAttr()));
          NExprType type = expr.type();

          if (type.equals(NIntExprType.INT_TYPE)) {
            return new NIntExprValue(node.intValue());
          } else if (type.equals(NBoolExprType.BOOL_TYPE)) {
            return new NBoolExprValue(node.booleanValue());
          } else if (type.equals(TUPLE_TYPE)) {
            return new NJsonTupleExprValue(node);
          }
          throw new IllegalStateException("unsupported type");
        } else {
          throw new IllegalStateException("only ref expression could been resolved");
        }
      }
    };
  }
}
