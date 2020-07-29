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

import com.amazon.opendistroforelasticsearch.sql.exception.ExpressionEvaluationException;

/**
 * Abstract ExprValue.
 */
public abstract class AbstractExprValue implements ExprValue {
  /**
   * The customize compareTo logic.
   */
  @Override
  public int compareTo(ExprValue other) {
    if (this.isNull() || this.isMissing()) {
      return this.compare(other);
    } else if (other.isNull() || other.isMissing()) {
      return -other.compareTo(this);
    }
<<<<<<< HEAD
    if ((this.isNumber() && other.isNumber()) || this.type() == other.type()) {
      return compare(other);
    } else {
=======
    if (!this.type().equals(other.type())) {
>>>>>>> develop
      throw new ExpressionEvaluationException(
          String.format(
              "compare expected value have same type, but with [%s, %s]",
              this.type(), other.type()));
    }
<<<<<<< HEAD
=======
    return compare(other);
>>>>>>> develop
  }

  /**
   * The customize equals logic.
   * The table below list the NULL and MISSING handling logic.
   * A       B       A == B
   * NULL    NULL    TRUE
   * NULL    MISSING FALSE
   * MISSING NULL    FALSE
   * MISSING MISSING TRUE
   */
  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    } else if (!(o instanceof ExprValue)) {
      return false;
    }
    ExprValue other = (ExprValue) o;
    if (this.isNull() || this.isMissing()) {
      return equal(other);
    } else if (other.isNull() || other.isMissing()) {
      return other.equals(this);
    } else {
      return equal(other);
    }
  }

  /**
   * The expression value compare.
   */
  public abstract int compare(ExprValue other);

  /**
   * The expression value equal.
   */
  public abstract boolean equal(ExprValue other);
}
