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

import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;
import com.amazon.opendistroforelasticsearch.sql.data.type.WideningTypeRule;
import com.amazon.opendistroforelasticsearch.sql.exception.ExpressionEvaluationException;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class FunctionResolverTest {
  @Mock
  private FunctionSignature exactlyMatchFS;
  @Mock
  private FunctionSignature bestMatchFS;
  @Mock
  private FunctionSignature leastMatchFS;
  @Mock
  private FunctionSignature notMatchFS;
  @Mock
  private FunctionSignature functionSignature;
  @Mock
  private FunctionBuilder exactlyMatchBuilder;
  @Mock
  private FunctionBuilder bestMatchBuilder;
  @Mock
  private FunctionBuilder leastMatchBuilder;
  @Mock
  private FunctionBuilder notMatchBuilder;

  private List<ExprType> typeList = Collections.emptyList();

  private FunctionName functionName = FunctionName.of("add");

  @Test
  void resolve_function_signature_exactly_match() {
    when(exactlyMatchFS.match(any(), any())).thenReturn(WideningTypeRule.TYPE_EQUAL);
    FunctionResolver resolver = new FunctionResolver(functionName,
        ImmutableMap.of(exactlyMatchFS, exactlyMatchBuilder));

    assertEquals(exactlyMatchBuilder, resolver.resolve(functionName, typeList));
  }

  @Test
  void resolve_function_signature_best_match() {
    when(bestMatchFS.match(any(), any())).thenReturn(1);
    when(leastMatchFS.match(any(), any())).thenReturn(2);
    FunctionResolver resolver = new FunctionResolver(functionName,
        ImmutableMap.of(bestMatchFS, bestMatchBuilder, leastMatchFS, leastMatchBuilder));

    assertEquals(bestMatchBuilder, resolver.resolve(functionName, typeList));
  }

  @Test
  void resolve_function_not_match() {
    when(notMatchFS.match(any(), any())).thenReturn(WideningTypeRule.IMPOSSIBLE_WIDENING);
    when(notMatchFS.formatTypes()).thenReturn("[INTEGER,INTEGER]");
    FunctionResolver resolver = new FunctionResolver(functionName,
        ImmutableMap.of(notMatchFS, notMatchBuilder));

    ExpressionEvaluationException exception = assertThrows(ExpressionEvaluationException.class,
        () -> resolver.resolve(functionName, Arrays.asList(BOOLEAN, BOOLEAN)));
    assertEquals("add function expected {[INTEGER,INTEGER]}, but get [BOOLEAN,BOOLEAN]",
        exception.getMessage());
  }
}