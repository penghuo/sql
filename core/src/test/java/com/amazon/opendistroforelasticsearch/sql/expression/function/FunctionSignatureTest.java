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

package com.amazon.opendistroforelasticsearch.sql.expression.function;

import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.DOUBLE;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.FLOAT;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.INTEGER;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.LONG;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.SHORT;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.STRING;
import static com.amazon.opendistroforelasticsearch.sql.expression.function.FunctionSignature.EXACTLY_MATCH;
import static com.amazon.opendistroforelasticsearch.sql.expression.function.FunctionSignature.NOT_MATCH;
import static com.amazon.opendistroforelasticsearch.sql.expression.function.FunctionSignature.var;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class FunctionSignatureTest {
  @Mock
  private List<ExprType> funcParamTypeList;

  private FunctionName ADD = FunctionName.of("add");

  private List<ExprType> unresolvedParamTypeList =
      Arrays.asList(INTEGER, FLOAT);

  @Test
  void signature_name_not_match() {
    FunctionSignature signature =
        new FunctionSignature(this.ADD, unresolvedParamTypeList);

    assertEquals(NOT_MATCH, signature.match(FunctionName.of(("diff")), funcParamTypeList));
  }

  @Test
  void signature_arguments_size_not_match() {
    FunctionSignature signature =
        new FunctionSignature(ADD, Arrays.asList(INTEGER, FLOAT));

    assertEquals(NOT_MATCH, signature.match(ADD, Arrays.asList(INTEGER)));

    signature =
        new FunctionSignature(ADD, Arrays.asList(INTEGER));

    assertEquals(NOT_MATCH, signature.match(ADD, Arrays.asList(INTEGER, FLOAT)));
  }

  @Test
  void signature_exactly_match() {
    FunctionSignature signature =
        new FunctionSignature(ADD, unresolvedParamTypeList);

    assertEquals(EXACTLY_MATCH, signature.match(ADD, unresolvedParamTypeList));
  }

  @Test
  void signature_not_match() {
    FunctionSignature signature =
        new FunctionSignature(ADD, unresolvedParamTypeList);

    assertEquals(NOT_MATCH, signature.match(ADD, Arrays.asList(ExprCoreType.STRING, ExprCoreType.STRING)));
  }

  @Test
  void signature_widening_match() {
    FunctionSignature signature =
        new FunctionSignature(ADD, Arrays.asList(FLOAT, FLOAT));
    assertEquals(2, signature.match(ADD, Arrays.asList(INTEGER, FLOAT)));

    signature =
        new FunctionSignature(ADD, Arrays.asList(LONG, LONG));
    assertEquals(2, signature.match(ADD, Arrays.asList(SHORT, LONG)));
  }

  @Test
  void var_signature_exactly_match() {
    FunctionSignature signature = var(ADD, INTEGER);

    assertEquals(0, signature.match(ADD, Arrays.asList(INTEGER, INTEGER)));
  }

  @Test
  void var_signature_not_match() {
    FunctionSignature signature = var(ADD, STRING);

    assertEquals(NOT_MATCH, signature.match(ADD, Arrays.asList(INTEGER, INTEGER)));
  }

  @Test
  void var_signature_arguments_size_not_match() {
    FunctionSignature signature = var(ADD, INTEGER, INTEGER);
    assertEquals(NOT_MATCH, signature.match(ADD, Arrays.asList(INTEGER)));
  }

  @Test
  void format_types() {
    FunctionSignature unresolvedFunSig =
        new FunctionSignature(ADD, unresolvedParamTypeList);

    assertEquals("[INTEGER,FLOAT]", unresolvedFunSig.formatTypes());
  }
}