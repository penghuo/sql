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

package com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.type;

import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.STRING;
import static com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.type.ExprKeywordType.KEYWORD;
import static com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.type.ExprMultiFieldTextType.MULTI_FIELD;
import static com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.type.ExprTextType.TEXT;
import static com.amazon.opendistroforelasticsearch.sql.expression.function.WideningTypeRule.IMPOSSIBLE_WIDENING;
import static com.amazon.opendistroforelasticsearch.sql.expression.function.WideningTypeRule.distance;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType;
import org.junit.jupiter.api.Test;

class CustomTypeTest {
  @Test
  public void testExprTextCompatibleType() {
    assertTrue(STRING.isCompatible(KEYWORD));
    assertFalse(KEYWORD.isCompatible(STRING));

    assertTrue(STRING.isCompatible(MULTI_FIELD));
    assertTrue(KEYWORD.isCompatible(MULTI_FIELD));
  }

  @Test
  public void customizeTypeWideningRuleTest() {
    assertEquals(1, distance(MULTI_FIELD, KEYWORD));
    assertEquals(IMPOSSIBLE_WIDENING, distance(KEYWORD, MULTI_FIELD));
    assertEquals(1, distance(MULTI_FIELD, TEXT));
    assertEquals(IMPOSSIBLE_WIDENING, distance(TEXT, MULTI_FIELD));
    assertEquals(1, distance(KEYWORD, STRING));
    assertEquals(IMPOSSIBLE_WIDENING, distance(STRING, KEYWORD));
    assertEquals(IMPOSSIBLE_WIDENING, distance(TEXT, STRING));
    assertEquals(IMPOSSIBLE_WIDENING, distance(STRING, TEXT));
  }
}