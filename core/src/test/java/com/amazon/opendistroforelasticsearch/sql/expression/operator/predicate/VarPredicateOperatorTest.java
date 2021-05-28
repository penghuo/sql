/*
 *     Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License").
 *     You may not use this file except in compliance with the License.
 *     A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *     or in the "license" file accompanying this file. This file is distributed
 *     on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *     express or implied. See the License for the specific language governing
 *     permissions and limitations under the License.
 *
 */

package com.amazon.opendistroforelasticsearch.sql.expression.operator.predicate;

import static com.amazon.opendistroforelasticsearch.sql.expression.DSL.literal;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazon.opendistroforelasticsearch.sql.expression.ExpressionTestBase;
import org.junit.jupiter.api.Test;

class VarPredicateOperatorTest extends ExpressionTestBase {

  @Test
  public void test() {
    assertTrue(
        dsl.in(literal(1), literal(1), literal(2), literal(3)).valueOf(valueEnv()).booleanValue());
    assertFalse(
        dsl.in(literal(4), literal(1), literal(2), literal(3)).valueOf(valueEnv()).booleanValue());
  }
}