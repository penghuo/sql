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

import com.google.common.base.Objects;
import lombok.RequiredArgsConstructor;

/**
 * Expression Number Value.
 */
@RequiredArgsConstructor
public abstract class AbstractExprNumberValue extends AbstractExprValue {
  private final Number value;

  @Override
<<<<<<< HEAD
  public boolean isNumber() {
    return true;
  }

  @Override
=======
>>>>>>> develop
  public Integer integerValue() {
    return value.intValue();
  }

  @Override
  public Long longValue() {
    return value.longValue();
  }

  @Override
  public Float floatValue() {
    return value.floatValue();
  }

  @Override
  public Double doubleValue() {
    return value.doubleValue();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value);
  }
}
