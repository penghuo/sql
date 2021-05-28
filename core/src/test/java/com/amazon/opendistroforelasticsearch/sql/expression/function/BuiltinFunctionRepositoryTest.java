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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType;
import com.amazon.opendistroforelasticsearch.sql.exception.ExpressionEvaluationException;
import com.amazon.opendistroforelasticsearch.sql.expression.Expression;
import com.amazon.opendistroforelasticsearch.sql.expression.env.Environment;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BuiltinFunctionRepositoryTest {
  @Mock
  private FunctionResolver mockFunctionResolver;
  @Mock
  private Map<FunctionName, FunctionResolver> mockMap;
  @Mock
  private FunctionName mockFunctionName;
  @Mock
  private FunctionBuilder functionExpressionBuilder;
  @Mock
  private Expression mockExpression;

  @Test
  void register() {
    BuiltinFunctionRepository repo = new BuiltinFunctionRepository(mockMap);
    when(mockFunctionResolver.getFunctionName()).thenReturn(mockFunctionName);
    repo.register(mockFunctionResolver);

    verify(mockMap, times(1)).put(mockFunctionName, mockFunctionResolver);
  }

  @Test
  void compile() {
    when(mockFunctionResolver.resolve(any(), any())).thenReturn(functionExpressionBuilder);
    when(mockMap.containsKey(any())).thenReturn(true);
    when(mockMap.get(any())).thenReturn(mockFunctionResolver);
    BuiltinFunctionRepository repo = new BuiltinFunctionRepository(mockMap);
    repo.register(mockFunctionResolver);

    repo.compile(mockFunctionName, Arrays.asList(mockExpression));
    verify(functionExpressionBuilder, times(1)).apply(any());
  }

  @Test
  @DisplayName("resolve registered function should pass")
  void resolve() {
    when(mockFunctionResolver.getFunctionName()).thenReturn(mockFunctionName);
    when(mockFunctionResolver.resolve(any(), any())).thenReturn(functionExpressionBuilder);
    when(mockMap.containsKey(mockFunctionName)).thenReturn(true);
    when(mockMap.get(mockFunctionName)).thenReturn(mockFunctionResolver);
    BuiltinFunctionRepository repo = new BuiltinFunctionRepository(mockMap);
    repo.register(mockFunctionResolver);

    assertEquals(functionExpressionBuilder, repo.resolve(mockFunctionName, Collections.emptyList()));
  }

  @Test
  @DisplayName("resolve unregistered function should throw exception")
  void resolve_unregistered() {
    BuiltinFunctionRepository repo = new BuiltinFunctionRepository(mockMap);
    when(mockMap.containsKey(any())).thenReturn(false);
    repo.register(mockFunctionResolver);

    ExpressionEvaluationException exception = assertThrows(ExpressionEvaluationException.class,
        () -> repo.resolve(FunctionName.of("unknown"), Collections.emptyList()));
    assertEquals("unsupported function name: unknown", exception.getMessage());
  }
}