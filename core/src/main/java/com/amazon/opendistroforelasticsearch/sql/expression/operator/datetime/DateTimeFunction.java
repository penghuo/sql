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

package com.amazon.opendistroforelasticsearch.sql.expression.operator.datetime;

import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprTimestampType.TIMESTAMP;

import com.amazon.opendistroforelasticsearch.sql.data.model.ExprDateValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprDateType;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;
import com.amazon.opendistroforelasticsearch.sql.expression.Expression;
import com.amazon.opendistroforelasticsearch.sql.expression.FunctionExpression;
import com.amazon.opendistroforelasticsearch.sql.expression.env.Environment;
import com.amazon.opendistroforelasticsearch.sql.expression.function.BuiltinFunctionRepository;
import com.amazon.opendistroforelasticsearch.sql.expression.function.FunctionBuilder;
import com.amazon.opendistroforelasticsearch.sql.expression.function.FunctionImplementation;
import com.amazon.opendistroforelasticsearch.sql.expression.function.FunctionName;
import com.amazon.opendistroforelasticsearch.sql.expression.function.FunctionResolver;
import com.amazon.opendistroforelasticsearch.sql.expression.function.FunctionSignature;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class DateTimeFunction {
  private static FunctionName DATE = FunctionName.of("date");

  /**
   * String operation.
   */
  public static void register(BuiltinFunctionRepository repository) {
    repository.register(date());
  }

  private static FunctionResolver date() {
    return new FunctionResolver(
        DATE,
        date(DATE, str -> str.length())
    );
  }

  private static Map<FunctionSignature, FunctionBuilder> date(
      FunctionName functionName,
      Function<String, Integer> lengthFunc) {
    Date date;

    ImmutableMap.Builder<FunctionSignature, FunctionBuilder> builder = new ImmutableMap.Builder<>();
    builder.put(
        new FunctionSignature(functionName, Arrays.asList(TIMESTAMP)),
        new FunctionBuilder() {
          @Override
          public FunctionImplementation apply(List<Expression> arguments) {
            return new FunctionExpression(functionName, arguments) {
              @Override
              public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
                Expression arg0 = arguments.get(0);
                ExprValue value = arg0.valueOf(valueEnv);
                return new ExprDateValue((Date) value.value());
              }

              @Override
              public ExprType type() {
                return ExprDateType.DATE;
              }
            };
          }
        });
    return builder.build();
  }
}
