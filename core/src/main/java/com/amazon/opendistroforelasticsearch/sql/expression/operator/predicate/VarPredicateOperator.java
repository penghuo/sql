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

import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.BOOLEAN;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.DOUBLE;

import com.amazon.opendistroforelasticsearch.sql.data.model.ExprBooleanValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.expression.function.BuiltinFunctionName;
import com.amazon.opendistroforelasticsearch.sql.expression.function.BuiltinFunctionRepository;
import com.amazon.opendistroforelasticsearch.sql.expression.function.FunctionDSL;
import com.amazon.opendistroforelasticsearch.sql.expression.function.FunctionResolver;
import java.util.List;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;

@UtilityClass
public class VarPredicateOperator {
  public static void register(BuiltinFunctionRepository repository) {
    repository.register(like());
  }

  private static FunctionResolver like() {
    return FunctionDSL.define(
        BuiltinFunctionName.IN.getName(),
        FunctionDSL.varImpl(VarPredicateOperator::isIn, BOOLEAN, DOUBLE, DOUBLE));
  }

  private static ExprBooleanValue isIn(ExprValue expr, List<ExprValue> setOfValues) {
    return ExprBooleanValue
        .of(setOfValues.stream().map(ExprValue::value).collect(Collectors.toSet())
            .contains(expr.value()));
  }
}
