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

import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.STRING;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.STRUCT;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazon.opendistroforelasticsearch.sql.data.model.ExprTupleValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;
import com.amazon.opendistroforelasticsearch.sql.expression.PathReferenceExpression;
import com.amazon.opendistroforelasticsearch.sql.expression.ReferenceExpression;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class PathExpressionTest {
  private static final Map<String, ExprType> MAPPING =
      new ImmutableMap.Builder<String, ExprType>()
          .put("employee", STRUCT)
          .put("employee.name", STRUCT)
          .put("employee.address", STRUCT)
          .put("employee.address.city", STRING)
          .put("employee.address.state", STRING)
          .build();
  private ElasticsearchExprValueFactory exprValueFactory =
      new ElasticsearchExprValueFactory(MAPPING);


  @Test
  public void compile() {
    String employeeStr =
        "{\"employee\":{\"name\":\"bob\",\"address\":{\"city\":\"sea\",\"state\":\"wa\"}}}";
    ExprTupleValue employee = exprValueFactory.construct(employeeStr);

    ReferenceExpression expression = compile("employee.address.city", STRING);
    ExprValue exprValue = expression.valueOf(employee.bindingTuples());

    assertEquals("employee.address.city", expression.toString());
    assertEquals(STRING, exprValue.type());
    assertEquals("sea", exprValue.value());
  }

  private ReferenceExpression compile(String path, ExprType type) {
    String[] pathArrays = path.split("\\.");
    return compile(Arrays.asList(pathArrays), type);
  }

  private ReferenceExpression compile(List<String> paths, ExprType type) {
    if (paths.size() == 1) {
      return new ReferenceExpression(paths.get(0), type);
    } else {
      return new PathReferenceExpression(paths.get(0), STRUCT,
          compile(paths.subList(1, paths.size()),
              type));
    }
  }
}
