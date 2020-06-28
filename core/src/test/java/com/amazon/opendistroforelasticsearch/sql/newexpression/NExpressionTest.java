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

import static com.amazon.opendistroforelasticsearch.sql.newexpression.type.NBoolExprType.BOOL_TYPE;
import static com.amazon.opendistroforelasticsearch.sql.newexpression.value.NExprValueUtils.toBoolean;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazon.opendistroforelasticsearch.sql.newexpression.type.NIntExprType;
import com.amazon.opendistroforelasticsearch.sql.newexpression.value.NExprValue;
import org.junit.jupiter.api.Test;

class NExpressionTest {
  @Test
  public void abs() {
    String json =
        "{\n"
            + "  \"mydb.r\": [\n"
            + "    3,\n"
            + "    \"x\"\n"
            + "  ],\n"
            + "  \"mydb.s\": [\n"
            + "    {\n"
            + "      \"a\": 1,\n"
            + "      \"b\": 2\n"
            + "    },\n"
            + "    {\n"
            + "      \"a\": 3\n"
            + "    }\n"
            + "  ],\n"
            + "  \"c\": {\n"
            + "    \"d\": {\n"
            + "      \"e\": 1\n"
            + "    }\n"
            + "  }\n"
            + "}";

    NExpression euqal =
        NExpressionFactory.intEqual(NExpressionFactory.ref("c/d/e", NIntExprType.INT_TYPE),
            NExpressionFactory.ref("mydb.s/0/a", NIntExprType.INT_TYPE));
    NExprValue exprValue = euqal.valueOf(bindingTuple(json));

    assertEquals(BOOL_TYPE, exprValue.type());
    assertTrue(toBoolean(exprValue));
  }

  NBindingTuple bindingTuple(String json) {
    return new NBindingTuple(json);
  }
}