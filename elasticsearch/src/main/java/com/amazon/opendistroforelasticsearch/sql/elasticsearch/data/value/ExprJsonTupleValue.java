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

package com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.value;

import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValueFactory;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;
import com.amazon.opendistroforelasticsearch.sql.expression.ReferenceExpression;
import com.amazon.opendistroforelasticsearch.sql.storage.bindingtuple.BindingTuple;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

@RequiredArgsConstructor
public class ExprJsonTupleValue implements ExprValue {
  private static ObjectMapper MAPPER = new ObjectMapper();
  private final ExprValueFactory<JsonNode> exprValueFactory;
  private final JsonNode jsonNode;

  public ExprJsonTupleValue(JsonNode jsonNode, ExprValueFactory<JsonNode> exprValueFactory) {
    this.jsonNode = jsonNode;
    this.exprValueFactory = exprValueFactory;
  }

  @SneakyThrows
  public ExprJsonTupleValue(String jsonString, ExprValueFactory<JsonNode> exprValueFactory) {
    this.jsonNode = MAPPER.readTree(jsonString);
    this.exprValueFactory = exprValueFactory;
  }

  @Override
  public ExprType type() {
    return ExprCoreType.STRUCT;
  }

  /**
   * Todo, what value should return.
   */
  @Override
  public Object value() {
    return MAPPER.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
    });
  }

  @Override
  public BindingTuple bindingTuples() {
    return new BindingTuple() {
      @Override
      public ExprValue resolve(ReferenceExpression expr) {
        JsonNode node = jsonNode.at(String.format("/%s", ((ReferenceExpression) expr).getAttr()));
        return exprValueFactory.create(node, expr.getType());
      }
    };
  }
}
