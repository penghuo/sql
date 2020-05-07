/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage;

import com.amazon.opendistroforelasticsearch.sql.data.model.ExprType;
import com.amazon.opendistroforelasticsearch.sql.planner.logical.AbstractPlanNodeVisitor;
import com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalFilter;
import com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalPlan;
import com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalRelation;
import com.amazon.opendistroforelasticsearch.sql.planner.physical.PhysicalPlan;
import com.amazon.opendistroforelasticsearch.sql.storage.Metadata;
import com.amazon.opendistroforelasticsearch.sql.storage.Table;
import com.google.common.collect.ImmutableMap;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

@RequiredArgsConstructor
public class ElasticsearchTable implements Table {

    private final Client client;
    private final String tableName;

    @Override
    public Metadata getMetaData() {
        return null;
    }

    @Override
    public PhysicalPlan find(LogicalPlan plan) {
        // Suppose Elasticsearch DSL doesn't support any filter or aggregation
        // so that no push down optimization can be performed.

        SearchRequestBuilder request = plan.accept(
                new AbstractPlanNodeVisitor<SearchRequestBuilder, Object>() {
                    @Override
                    public SearchRequestBuilder visitRelation(LogicalRelation node, Object context) {
                        return client.prepareSearch(node.getRelationName());
                    }

                    @Override
                    public SearchRequestBuilder visitFilter(LogicalFilter plan, Object context) {
                        return super.visitFilter(plan, context);
                    }
                },
                null);
        request.setSize(100);
        return new ElasticsearchIndexScan(request);
    }

    @Override
    public Map<String, ExprType> getFieldTypes() {
        try {
            GetMappingsRequest request = new GetMappingsRequest();
            request.indices(tableName);

            GetMappingsResponse getMappingsResponse = client.admin().indices().getMappings(request).get();
            Map<String, Map<String, Object>> mappings = (Map<String, Map<String, Object>>) getMappingsResponse
                    .mappings()
                    .get(tableName)
                    .get("_doc")
                    .getSourceAsMap()
                    .get("properties");
            ImmutableMap.Builder<String, ExprType> mapBuilder = new ImmutableMap.Builder<>();
            flatMappings(mappings, Optional.empty(), (k, v) -> mapBuilder.put(k, ExprType.typeOf(v)));
            ImmutableMap<String, ExprType> result = mapBuilder.build();
            return result;
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException();
        }
    }

    private void flatMappings(Map<String, Map<String, Object>> mappings,
                              Optional<String> path,
                              BiConsumer<String, String> func) {
        mappings.forEach(
                (fieldName, mapping) -> {
                    String fullFieldName = path.map(s -> s + "." + fieldName).orElse(fieldName);
                    String type = (String) mapping.getOrDefault("type", "object");
                    func.accept(fullFieldName, type);

                    if (mapping.containsKey("fields")) {
                        ((Map<String, Map<String, Object>>) mapping.get("fields")).forEach(
                                (innerFieldName, innerMapping) ->
                                        func.accept(fullFieldName + "." + innerFieldName,
                                                (String) innerMapping.getOrDefault("type", "object"))
                        );
                    }

                    if (mapping.containsKey("properties")) {
                        flatMappings(
                                (Map<String, Map<String, Object>>) mapping.get("properties"),
                                Optional.of(fullFieldName),
                                func
                        );
                    }
                }
        );
    }

}
