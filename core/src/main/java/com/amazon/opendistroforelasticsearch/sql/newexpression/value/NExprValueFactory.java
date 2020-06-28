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

import static com.amazon.opendistroforelasticsearch.sql.newexpression.value.NExprValueUtils.toInteger;

public class NExprValueFactory {

  public static NExprValue intEqual(NExprValue v1, NExprValue v2) {
    Integer intV1 = toInteger(v1);
    Integer intV2 = toInteger(v2);
    return new NBoolExprValue(intV1.equals(intV2));
  }
}
