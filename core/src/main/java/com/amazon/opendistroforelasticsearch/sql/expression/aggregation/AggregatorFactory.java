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

package com.amazon.opendistroforelasticsearch.sql.expression.aggregation;

import com.amazon.opendistroforelasticsearch.sql.data.model.ExprType;
import com.amazon.opendistroforelasticsearch.sql.expression.function.BuiltinFunctionName;
import com.amazon.opendistroforelasticsearch.sql.expression.function.FunctionSignature;
import com.google.common.collect.ImmutableMap;

import java.util.Arrays;

public class AggregatorFactory {

    private static AggregatorResolver avg() {
        return new AggregatorResolver(
                BuiltinFunctionName.AVG.getName(),
                new ImmutableMap.Builder<FunctionSignature, AggregatorBuilder>()
                        .put(new FunctionSignature(BuiltinFunctionName.AVG.getName(), Arrays.asList(ExprType.DOUBLE)), arguments -> new AvgAggregator(arguments))
                        .build()
        );
    }
}
