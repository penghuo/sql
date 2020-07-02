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

package com.amazon.opendistroforelasticsearch.sql.data.model;

import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.ARRAY;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.DOUBLE;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.FLOAT;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.INTEGER;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.LONG;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.STRING;
import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.STRUCT;

import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;

public abstract class CoreExprValueFactory<S> implements ExprValueFactory<S> {
  @Override
  public ExprValue create(S source, ExprType type) {
    if (type.equals(INTEGER)) {
      return createInteger(source, type);
    } else if (type.equals(LONG)) {
      return createLong(source, type);
    } else if (type.equals(FLOAT)) {
      return createFloat(source, type);
    } else if (type.equals(DOUBLE)) {
      return createDouble(source, type);
    } else if (type.equals(STRING)) {
      return createString(source, type);
    } else if (type.equals(STRUCT)) {
      return createTuple(source, type);
    } else if (type.equals(ARRAY)) {
      return createCollection(source, type);
    } else {
      return createCustomize(source, type);
    }
  }

  public abstract ExprValue createInteger(S source, ExprType type);

  public abstract ExprValue createLong(S source, ExprType type);

  public abstract ExprValue createFloat(S source, ExprType type);

  public abstract ExprValue createDouble(S source, ExprType type);

  public abstract ExprValue createString(S source, ExprType type);

  public abstract ExprValue createTuple(S source, ExprType type);

  public abstract ExprValue createCollection(S source, ExprType type);

  public abstract ExprValue createCustomize(S source, ExprType type);
}
