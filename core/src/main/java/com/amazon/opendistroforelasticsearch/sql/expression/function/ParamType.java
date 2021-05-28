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

package com.amazon.opendistroforelasticsearch.sql.expression.function;

import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;
import java.util.Iterator;
import java.util.List;
import lombok.RequiredArgsConstructor;

public class ParamType {

  List<Iterator<ExprType>> typeList;



  @RequiredArgsConstructor
  public static class SingleType implements Iterator<ExprType> {
    private final ExprType type;
    private boolean toggle = true;

    @Override
    public boolean hasNext() {
      if (toggle) {
        toggle = false;
        return true;
      }
      return false;
    }

    @Override
    public ExprType next() {
      return type;
    }
  }


  @RequiredArgsConstructor
  public static class VarType implements Iterator<ExprType> {
    private final ExprType type;

    @Override
    public boolean hasNext() {
      return true;
    }

    @Override
    public ExprType next() {
      return type;
    }
  }
}
