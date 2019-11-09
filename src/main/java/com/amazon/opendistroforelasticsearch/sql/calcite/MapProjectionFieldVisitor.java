/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */
package com.amazon.opendistroforelasticsearch.sql.calcite;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/**
 * Visitor that extracts the actual field name from an item expression.
 */
class MapProjectionFieldVisitor extends RexVisitorImpl<String> {

  static final MapProjectionFieldVisitor INSTANCE = new MapProjectionFieldVisitor();

  private MapProjectionFieldVisitor() {
    super(true);
  }

  @Override public String visitCall(RexCall call) {
    if (call.op == SqlStdOperatorTable.ITEM) {
      return ((RexLiteral) call.getOperands().get(1)).getValueAs(String.class);
    }
    return super.visitCall(call);
  }
}

// End MapProjectionFieldVisitor.java
