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

package com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage;

import static com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.script.ExpressionScriptEngine.EXPRESSION_LANG_NAME;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.script.Script.DEFAULT_SCRIPT_TYPE;

import com.amazon.opendistroforelasticsearch.sql.common.setting.Settings;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.client.ElasticsearchClient;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.type.ElasticsearchDataType;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.value.ElasticsearchExprIpValue;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.value.ElasticsearchExprValueFactory;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.mapping.IndexMapping;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.script.aggregation.AggregationQueryBuilder;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.script.filter.FilterQueryBuilder;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.serialization.DefaultExpressionSerializer;
import com.amazon.opendistroforelasticsearch.sql.expression.Expression;
import com.amazon.opendistroforelasticsearch.sql.expression.ReferenceExpression;
import com.amazon.opendistroforelasticsearch.sql.expression.aggregation.Aggregator;
import com.amazon.opendistroforelasticsearch.sql.planner.DefaultImplementor;
import com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalAggregation;
import com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalFilter;
import com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalPlan;
import com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalRelation;
import com.amazon.opendistroforelasticsearch.sql.planner.physical.PhysicalPlan;
import com.amazon.opendistroforelasticsearch.sql.storage.Table;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

/** Elasticsearch table (index) implementation. */
@RequiredArgsConstructor
public class ElasticsearchIndex implements Table {

  /**
   * Type mapping from Elasticsearch data type to expression type in our type system in query
   * engine. TODO: geo, ip etc.
   */
  private static final Map<String, ExprType> ES_TYPE_TO_EXPR_TYPE_MAPPING =
      ImmutableMap.<String, ExprType>builder()
          .put("text", ElasticsearchDataType.ES_TEXT)
          .put("text_keyword", ElasticsearchDataType.ES_TEXT_KEYWORD)
          .put("keyword", ExprCoreType.STRING)
          .put("integer", ExprCoreType.INTEGER)
          .put("long", ExprCoreType.LONG)
          .put("float", ExprCoreType.FLOAT)
          .put("half_float", ExprCoreType.FLOAT)
          .put("double", ExprCoreType.DOUBLE)
          .put("boolean", ExprCoreType.BOOLEAN)
          .put("nested", ExprCoreType.ARRAY)
          .put("object", ExprCoreType.STRUCT)
          .put("date", ExprCoreType.TIMESTAMP)
          .put("ip", ElasticsearchDataType.ES_IP)
          .put("geo_point", ElasticsearchDataType.ES_GEO_POINT)
          .build();

  /** Elasticsearch client connection. */
  private final ElasticsearchClient client;

  private final Settings settings;

  /** Current Elasticsearch index name. */
  private final String indexName;

  /*
   * TODO: Assume indexName doesn't have wildcard.
   *  Need to either handle field name conflicts
   *   or lazy evaluate when query engine pulls field type.
   */
  @Override
  public Map<String, ExprType> getFieldTypes() {
    Map<String, ExprType> fieldTypes = new HashMap<>();
    Map<String, IndexMapping> indexMappings = client.getIndexMappings(indexName);
    for (IndexMapping indexMapping : indexMappings.values()) {
      fieldTypes.putAll(indexMapping.getAllFieldTypes(this::transformESTypeToExprType));
    }
    return fieldTypes;
  }

  /** TODO: Push down operations to index scan operator as much as possible in future. */
  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    ElasticsearchIndexScan indexScan = new ElasticsearchIndexScan(client, settings, indexName,
        new ElasticsearchExprValueFactory(getFieldTypes()));

    /*
     * Visit logical plan with index scan as context so logical operators visited, such as
     * aggregation, filter, will accumulate (push down) Elasticsearch query and aggregation DSL on
     * index scan.
     */
    return plan.accept(new DefaultImplementor<ElasticsearchIndexScan>() {
          @Override
          public PhysicalPlan visitFilter(LogicalFilter node, ElasticsearchIndexScan context) {
            // For now (without optimizer), only push down filter close to relation
            if (!(node.getChild().get(0) instanceof LogicalRelation)) {
              return super.visitFilter(node, context);
            }

            FilterQueryBuilder queryBuilder =
                new FilterQueryBuilder(new DefaultExpressionSerializer());

            QueryBuilder query = queryBuilder.build(node.getCondition());

            context.pushDown(query);
            return visitChild(node, context);
          }

          @Override
          public PhysicalPlan visitAggregation(LogicalAggregation node,
                                              ElasticsearchIndexScan context) {
            DefaultExpressionSerializer serializer = new DefaultExpressionSerializer();
            List<Aggregator> aggregatorList = node.getAggregatorList();
            Expression expression = (Expression) aggregatorList.get(0).getArguments().get(0);
            Script aggScript = new Script(
                DEFAULT_SCRIPT_TYPE, EXPRESSION_LANG_NAME, serializer.serialize(expression),
                emptyMap());

            List<Expression> groupByList = node.getGroupByList();
            Expression groupExpression = groupByList.get(0);
            Script groupScript = new Script(
                DEFAULT_SCRIPT_TYPE, EXPRESSION_LANG_NAME, serializer.serialize(groupExpression),
                emptyMap());

            List<CompositeValuesSourceBuilder<?>> valuesSourceBuilders =
                Arrays.asList(new TermsValuesSourceBuilder("groupField").script(groupScript));

            CompositeAggregationBuilder aggregationBuilder =
                AggregationBuilders.composite("composite_buckets", valuesSourceBuilders)
                    .subAggregation(AggregationBuilders
                        .avg("avg")
                        .script(aggScript));

            context.pushDownAggregation(aggregationBuilder);

            return visitChild(node, context);
          }

          @Override
          public PhysicalPlan visitRelation(LogicalRelation node, ElasticsearchIndexScan context) {
            return indexScan;
          }
        },
        indexScan);
  }

  private ExprType transformESTypeToExprType(String esType) {
    return ES_TYPE_TO_EXPR_TYPE_MAPPING.getOrDefault(esType, ExprCoreType.UNKNOWN);
  }
}
