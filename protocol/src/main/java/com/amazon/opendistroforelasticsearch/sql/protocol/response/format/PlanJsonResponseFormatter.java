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

package com.amazon.opendistroforelasticsearch.sql.protocol.response.format;

import com.amazon.opendistroforelasticsearch.sql.protocol.response.PlanQueryResult;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

public class PlanJsonResponseFormatter extends JsonResponseFormatter<PlanQueryResult> {


  public PlanJsonResponseFormatter(JsonResponseFormatter.Style style) {
    super(style);
  }

  @Override
  public Object buildJsonObject(PlanQueryResult response) {
    JsonResponse.JsonResponseBuilder json = JsonResponse.builder();

    json.total(response.size())
        .size(response.size());

    json.datarows(fetchDataRows(response));
    return json.build();
  }

  private Object[] fetchDataRows(PlanQueryResult response) {
    Object[] rows = new Object[response.size()];
    int i = 0;
    for (Object values : response) {
      rows[i++] = values;
    }
    return rows;
  }

  /**
   * org.json requires these inner data classes be public (and static)
   */
  @Builder
  @Getter
  public static class JsonResponse {
    private final Object[] datarows;

    private long total;
    private long size;
  }

  @RequiredArgsConstructor
  @Getter
  public static class Column {
    private final String name;
  }

}
