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

import static com.amazon.opendistroforelasticsearch.sql.newexpression.type.NBoolExprType.BOOL_TYPE;
import static com.amazon.opendistroforelasticsearch.sql.newexpression.type.NIntExprType.INT_TYPE;

public class NExprValueUtils {

  public static Integer toInteger(NExprValue value) {
    if (INT_TYPE.compatible(value.type())) {
      return ((Number) value.getValue()).intValue();
    } else {
      throw new RuntimeException("invalid type" + value.type());
    }
  }

  public static Boolean toBoolean(NExprValue value) {
    if (BOOL_TYPE == value.type()) {
      return (Boolean) value.getValue();
    } else {
      throw new RuntimeException("invalid type" + value.type());
    }
  }
}
