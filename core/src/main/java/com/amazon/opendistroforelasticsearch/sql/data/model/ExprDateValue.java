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

import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprTimestampType.TIMESTAMP;

import com.amazon.opendistroforelasticsearch.sql.data.type.ExprDateType;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;
import java.util.Date;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

@EqualsAndHashCode
@RequiredArgsConstructor
public class ExprDateValue implements ExprValue {
  private final Date date;

  @Override
  public Object value() {
    return DateFormatUtils.format(date, "yyyy-MM-dd");
  }

  @Override
  public ExprType type() {
    return ExprDateType.DATE;
  }
}
