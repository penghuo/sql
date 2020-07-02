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

package com.amazon.opendistroforelasticsearch.sql.data.type;

import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.UNKNOWN;

import java.util.Arrays;
import java.util.List;

public interface ExprType {

  /**
   * Dummy.
   */
  default boolean isCompatible(ExprType other) {
    if (this.equals(other)) {
      return true;
    } else {
      if (other == UNKNOWN) {
        return false;
      }
      for (ExprType parentTypeOfOther : other.getParent()) {
        if (isCompatible(parentTypeOfOther)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * Dummy.
   */
  default List<ExprType> getParent() {
    return Arrays.asList(UNKNOWN);
  }

  default String name() {
    return toString();
  }
}
