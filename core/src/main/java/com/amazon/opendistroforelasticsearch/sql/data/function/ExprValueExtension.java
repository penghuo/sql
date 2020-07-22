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

package com.amazon.opendistroforelasticsearch.sql.data.function;

import static com.amazon.opendistroforelasticsearch.sql.data.model.ExprValueUtils.LITERAL_FALSE;
import static com.amazon.opendistroforelasticsearch.sql.data.model.ExprValueUtils.LITERAL_MISSING;
import static com.amazon.opendistroforelasticsearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static com.amazon.opendistroforelasticsearch.sql.data.model.ExprValueUtils.LITERAL_TRUE;


import com.amazon.opendistroforelasticsearch.sql.data.model.ExprBooleanValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ExprValueExtension {
  private static Table<ExprValue, ExprValue, ExprValue> equalTable =
      new ImmutableTable.Builder<ExprValue, ExprValue, ExprValue>()
          .put(LITERAL_NULL, LITERAL_NULL, LITERAL_TRUE)
          .put(LITERAL_NULL, LITERAL_MISSING, LITERAL_FALSE)
          .put(LITERAL_MISSING, LITERAL_NULL, LITERAL_FALSE)
          .put(LITERAL_MISSING, LITERAL_MISSING, LITERAL_TRUE)
          .build();

  public ExprValue stringEqual(ExprValue arg1, ExprValue arg2) {
    if (equalTable.contains(arg1, arg2)) {
      return equalTable.get(arg1, arg2);
    } else if (arg1.isMissing() || arg1.isNull() || arg2.isMissing() || arg2.isNull()) {
      return LITERAL_FALSE;
    } else {
      return ExprBooleanValue.of(arg1.string().equals(arg2.string()));
    }
  }
}
