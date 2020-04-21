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

package com.amazon.opendistroforelasticsearch.sql.expression.scalar;

import com.amazon.opendistroforelasticsearch.sql.data.model.ExprType;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.expression.DSL;
import com.amazon.opendistroforelasticsearch.sql.expression.config.FunctionConfig;
import com.amazon.opendistroforelasticsearch.sql.expression.env.ExprTypeEnv;
import com.amazon.opendistroforelasticsearch.sql.expression.env.ExprValueEnv;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {FunctionConfig.class})
public class FunctionTestBase {
    @Autowired
    protected DSL dsl;

    private ExprValueEnv emptyValueEnv = new ExprValueEnv(ImmutableMap.of());

    private ExprTypeEnv emptyTypeEnv = new ExprTypeEnv(ImmutableMap.of());

    protected ExprValueEnv emptyValueEnv() {
        return emptyValueEnv;
    }

    protected ExprTypeEnv emptyTypeEnv() {
        return emptyTypeEnv;
    }
}
