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

import com.amazon.opendistroforelasticsearch.sql.exception.ExpressionEvaluationException;
import com.amazon.opendistroforelasticsearch.sql.expression.function.FunctionName;
import com.amazon.opendistroforelasticsearch.sql.expression.function.FunctionSignature;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Singular;

import java.util.AbstractMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class AggregatorResolver {
    @Getter
    private final FunctionName functionName;
    @Singular("functionBundle")
    private final Map<FunctionSignature, AggregatorBuilder> functionBundle;

    public AggregatorBuilder resolve(FunctionSignature unresolvedSignature) {
        PriorityQueue<Map.Entry<Integer, FunctionSignature>> functionMatchQueue = new PriorityQueue<>(
                Map.Entry.comparingByKey());

        for (FunctionSignature functionSignature : functionBundle.keySet()) {
            functionMatchQueue.add(
                    new AbstractMap.SimpleEntry<>(unresolvedSignature.match(functionSignature), functionSignature));
        }
        Map.Entry<Integer, FunctionSignature> bestMatchEntry = functionMatchQueue.peek();
        if (FunctionSignature.NOT_MATCH.equals(bestMatchEntry.getKey())) {
            throw new ExpressionEvaluationException(
                    String.format("%s function expected %s, but get %s", functionName,
                            formatFunctions(functionBundle.keySet()),
                            unresolvedSignature.formatTypes()
                    ));
        } else {
            return functionBundle.get(bestMatchEntry.getValue());
        }
    }

    private String formatFunctions(Set<FunctionSignature> functionSignatures) {
        return functionSignatures.stream().map(FunctionSignature::formatTypes)
                .collect(Collectors.joining(",", "{", "}"));
    }


}
