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
import com.amazon.opendistroforelasticsearch.sql.expression.ReferenceExpression;
import com.amazon.opendistroforelasticsearch.sql.storage.bindingtuple.BindingTuple;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashSet;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

@RequiredArgsConstructor
public class JsonBindingTuple extends BindingTuple {
  private static ObjectMapper MAPPER = new ObjectMapper();
  private final ExprValueFactory<JsonNode> exprValueFactory;
  private final JsonNode jsonNode;

  public JsonBindingTuple(JsonNode jsonNode, ExprValueFactory<JsonNode> exprValueFactory) {
    this.jsonNode = jsonNode;
    this.exprValueFactory = exprValueFactory;

  }

  @SneakyThrows
  public JsonBindingTuple(String jsonString, ExprValueFactory<JsonNode> exprValueFactory) {
    this.jsonNode = MAPPER.readTree(jsonString);
    this.exprValueFactory = exprValueFactory;
  }

  @Override
  public ExprValue resolve(ReferenceExpression ref) {
    JsonNode node = jsonNode.at(String.format("/%s", ref.getAttr()));
    return exprValueFactory.create(node, ref.getType());
  }

  @Override
  public Set<String> bindingNames() {
    Set<String> set = new HashSet<>();
    jsonNode.fieldNames().forEachRemaining(set::add);
    return set;
  }
}
