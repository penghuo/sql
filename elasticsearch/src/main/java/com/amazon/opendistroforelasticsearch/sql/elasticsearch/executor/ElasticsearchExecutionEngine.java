/*
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package com.amazon.opendistroforelasticsearch.sql.elasticsearch.executor;

import com.amazon.opendistroforelasticsearch.sql.common.response.ResponseListener;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.client.ElasticsearchClient;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.executor.protector.ExecutionProtector;
import com.amazon.opendistroforelasticsearch.sql.executor.ExecutionEngine;
import com.amazon.opendistroforelasticsearch.sql.planner.physical.PhysicalPlan;
import com.amazon.opendistroforelasticsearch.sql.planner.physical.ProjectOperator;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;

/** Elasticsearch execution engine implementation. */
@RequiredArgsConstructor
public class ElasticsearchExecutionEngine implements ExecutionEngine {

  private final ElasticsearchClient client;

  private final ExecutionProtector executionProtector;

  @Override
  public void execute(PhysicalPlan physicalPlan, ResponseListener<QueryResponse> listener) {
    PhysicalPlan plan = executionProtector.protect(physicalPlan);
    client.schedule(
        () -> {
          try {
            if (plan instanceof ProjectOperator) {
              List<ExprValue> result = new ArrayList<>();

              ProjectOperator project = (ProjectOperator) plan;
              Schema schema =
                  new Schema(project.getProjectList().stream()
                      .map(expr -> new Schema.Column(expr.getName(),
                          expr.getName(), expr.type())).collect(Collectors.toList()));

              plan.open();

              while (plan.hasNext()) {
                result.add(plan.next());
              }

              QueryResponse response = new QueryResponse(schema, result);
              listener.onResponse(response);
            } else {
              throw new IllegalStateException("[BUG] The first operator must be project");
            }
          } catch (Exception e) {
            listener.onFailure(e);
          } finally {
            plan.close();
          }
        });
  }
}
