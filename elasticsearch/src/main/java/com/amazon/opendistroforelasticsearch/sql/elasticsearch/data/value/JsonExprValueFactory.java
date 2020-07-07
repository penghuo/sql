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

package com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.value;

import static com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.type.ExprKeywordType.KEYWORD;
import static com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.type.ExprMultiFieldTextType.MULTI_FIELD;
import static com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.type.ExprTextType.TEXT;

import com.amazon.opendistroforelasticsearch.sql.data.model.CoreExprValueFactory;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprDoubleValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprFloatValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprIntegerValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprLongValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprStringValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprTimestampValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.SneakyThrows;
import org.apache.commons.lang3.time.DateUtils;

public class JsonExprValueFactory extends CoreExprValueFactory<JsonNode> {
  private static final String FORMAT_DOT_DATE_AND_TIME = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
  private static final String FORMAT_DOT_KIBANA_SAMPLE_DATA_LOGS_EXCEPTION = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
  private static final String FORMAT_DOT_KIBANA_SAMPLE_DATA_FLIGHTS_EXCEPTION = "yyyy-MM-dd'T'HH:mm:ss";
  private static final String FORMAT_DOT_KIBANA_SAMPLE_DATA_FLIGHTS_EXCEPTION_NO_TIME = "yyyy-MM-dd'T'";
  private static final String FORMAT_DOT_KIBANA_SAMPLE_DATA_ECOMMERCE_EXCEPTION = "yyyy-MM-dd'T'HH:mm:ssXXX";
  private static final String FORMAT_DATE = "yyyy-MM-dd";


  @Override
  public ExprValue createInteger(JsonNode source, ExprType type) {
    return new ExprIntegerValue(source.intValue());
  }

  @Override
  public ExprValue createLong(JsonNode source, ExprType type) {
    return new ExprLongValue(source.longValue());
  }

  @Override
  public ExprValue createFloat(JsonNode source, ExprType type) {
    return new ExprFloatValue(source.floatValue());
  }

  @Override
  public ExprValue createDouble(JsonNode source, ExprType type) {
    return new ExprDoubleValue(source.doubleValue());
  }

  @Override
  public ExprValue createString(JsonNode source, ExprType type) {
    return new ExprStringValue(source.textValue());
  }

  @SneakyThrows
  @Override
  public ExprValue createTimestamp(JsonNode source, ExprType type) {
    return new ExprTimestampValue(
        DateUtils.parseDate(source.textValue(), FORMAT_DATE,
            FORMAT_DOT_KIBANA_SAMPLE_DATA_LOGS_EXCEPTION,
            FORMAT_DOT_KIBANA_SAMPLE_DATA_FLIGHTS_EXCEPTION,
            FORMAT_DOT_KIBANA_SAMPLE_DATA_FLIGHTS_EXCEPTION_NO_TIME,
            FORMAT_DOT_KIBANA_SAMPLE_DATA_ECOMMERCE_EXCEPTION,
            FORMAT_DOT_DATE_AND_TIME));
  }

  @Override
  public ExprValue createTuple(JsonNode source, ExprType type) {
    return new ExprJsonTupleValue(this, source);
  }

  //todo
  @Override
  public ExprValue createCollection(JsonNode source, ExprType type) {
    throw new IllegalStateException("todo, unsupported collection now");
  }

  @Override
  public ExprValue createCustomize(JsonNode source, ExprType type) {
    if (type.equals(KEYWORD)) {
      return new ExprStringValue(source.textValue());
    } else if (type.equals(TEXT)) {
      return new ExprStringValue(source.textValue());
    } else if (type.equals(MULTI_FIELD)) {
      return new ExprStringValue(source.textValue());
    }
    throw new IllegalStateException("unsupported operation");
  }
}
