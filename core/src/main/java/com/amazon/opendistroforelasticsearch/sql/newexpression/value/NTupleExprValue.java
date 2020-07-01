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

package com.amazon.opendistroforelasticsearch.sql.newexpression.value;

import static com.amazon.opendistroforelasticsearch.sql.newexpression.type.NTupleExprType.TUPLE_TYPE;

import com.amazon.opendistroforelasticsearch.sql.newexpression.NRefExpression;
import com.amazon.opendistroforelasticsearch.sql.newexpression.bindings.NBindingTuple;
import com.amazon.opendistroforelasticsearch.sql.newexpression.type.NExprType;
import java.util.LinkedHashMap;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class NTupleExprValue implements NExprValue {
  @Getter
  private final LinkedHashMap<String, NExprValue> valueMap;

  @Override
  public NExprType type() {
    return TUPLE_TYPE;
  }

  @Override
  public Object getValue() {
    return valueMap;
  }

  @Override
  public NBindingTuple bindingTuples() {
    return new NBindingTuple() {
      @Override
      public NExprValue resolve(NRefExpression name) {
        return valueMap.get(name.getAttr());
      }
    };
  }
}
