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

import static org.elasticsearch.search.sort.FieldSortBuilder.DOC_FIELD_NAME;
import static org.elasticsearch.search.sort.SortOrder.ASC;

import com.amazon.opendistroforelasticsearch.sql.common.setting.Settings;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.client.ElasticsearchClient;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.value.ElasticsearchExprValueFactory;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.request.ElasticsearchQueryRequest;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.request.ElasticsearchRequest;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.response.ElasticsearchResponse;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.s3.S3Scan;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.security.SecurityAccess;
import com.amazon.opendistroforelasticsearch.sql.expression.ReferenceExpression;
import com.amazon.opendistroforelasticsearch.sql.storage.TableScanOperator;
import com.amazon.opendistroforelasticsearch.sql.storage.bindingtuple.BindingTuple;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;

/**
 * Elasticsearch index scan operator.
 */
@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
@ToString(onlyExplicitlyIncluded = true)
public class ElasticsearchIndexScan extends TableScanOperator {

  private static final Logger log = LogManager.getLogger(ElasticsearchIndexScan.class);

  /** Elasticsearch client. */
  private final ElasticsearchClient client;

  /**
   * Search request.
   */
  @EqualsAndHashCode.Include
  @Getter
  @ToString.Include
  private final ElasticsearchRequest request;

  /**
   * Search response for current batch.
   */
  private Iterator<ExprValue> iterator;

  private S3Scan s3Scan;

  /**
   * Todo.
   */
  public ElasticsearchIndexScan(ElasticsearchClient client,
                                Settings settings, String indexName,
                                ElasticsearchExprValueFactory exprValueFactory) {
    this.client = client;
    this.request = new ElasticsearchQueryRequest(indexName,
        settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT), exprValueFactory);
  }

  @Override
  public void open() {
    super.open();

    // For now pull all results immediately once open
    List<ElasticsearchResponse> responses = new ArrayList<>();
    ElasticsearchResponse response = client.search(request);
    while (!response.isEmpty()) {
      responses.add(response);
      response = client.search(request);
    }
    Iterator<ExprValue> logStream =
        Iterables.concat(responses.toArray(new ElasticsearchResponse[0])).iterator();

    S3Scan s3Scan = new S3Scan(s3Objects(logStream));
    s3Scan.open();
    // Todo. Return 100 records now for testing purpose
    iterator = Iterators.limit(s3Scan, 100);
  }

  private List<Pair<String, String>> s3Objects(Iterator<ExprValue> logStream) {
    List<Pair<String, String>> s3Objects = new ArrayList<>();
    logStream.forEachRemaining(value -> {
      final BindingTuple tuple = value.bindingTuples();
      final ExprValue bucket =
          tuple.resolve(new ReferenceExpression("meta.bucket", ExprCoreType.STRING));
      final ExprValue object =
          tuple.resolve(new ReferenceExpression("meta.object", ExprCoreType.STRING));
      log.info("bucket {}, object {}", bucket.stringValue(), object.stringValue());
      s3Objects.add(Pair.of(bucket.stringValue(), object.stringValue()));
    });
    return s3Objects;
  }

  private <T> T doPrivileged(PrivilegedExceptionAction<T> action) {
    try {
      return SecurityAccess.doPrivileged(action);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to perform privileged action", e);
    }
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return iterator.next();
  }

  /**
   * Push down query to DSL request.
   * @param query  query request
   */
  public void pushDown(QueryBuilder query) {
    SearchSourceBuilder source = request.getSourceBuilder();
    QueryBuilder current = source.query();

    if (current == null) {
      source.query(query);
    } else {
      if (isBoolFilterQuery(current)) {
        ((BoolQueryBuilder) current).filter(query);
      } else {
        source.query(QueryBuilders.boolQuery()
                                  .filter(current)
                                  .filter(query));
      }
    }

    if (source.sorts() == null) {
      source.sort(DOC_FIELD_NAME, ASC); // Make sure consistent order
    }
  }

  /**
   * Push down aggregation to DSL request.
   * @param aggregationBuilderList aggregation query.
   */
  public void pushDownAggregation(List<AggregationBuilder> aggregationBuilderList) {
    SearchSourceBuilder source = request.getSourceBuilder();
    aggregationBuilderList.forEach(aggregationBuilder -> source.aggregation(aggregationBuilder));
    source.size(0);
  }

  /**
   * Push down sort to DSL request.
   *
   * @param sortBuilders sortBuilders.
   */
  public void pushDownSort(List<SortBuilder<?>> sortBuilders) {
    SearchSourceBuilder source = request.getSourceBuilder();
    for (SortBuilder<?> sortBuilder : sortBuilders) {
      source.sort(sortBuilder);
    }
  }

  /**
   * Push down size (limit) and from (offset) to DSL request.
   */
  public void pushDownLimit(Integer limit, Integer offset) {
    SearchSourceBuilder sourceBuilder = request.getSourceBuilder();
    sourceBuilder.from(offset).size(limit);
  }

  /**
   * Push down project list to DSL requets.
   */
  public void pushDownProjects(Set<ReferenceExpression> projects) {
    SearchSourceBuilder sourceBuilder = request.getSourceBuilder();
    final Set<String> projectsSet =
        projects.stream().map(ReferenceExpression::getAttr).collect(Collectors.toSet());
    sourceBuilder.fetchSource(projectsSet.toArray(new String[0]), new String[0]);
  }

  public void pushTypeMapping(Map<String, ExprType> typeMapping) {
    request.getExprValueFactory().setTypeMapping(typeMapping);
  }

  @Override
  public void close() {
    super.close();

    client.cleanup(request);
  }

  private boolean isBoolFilterQuery(QueryBuilder current) {
    return (current instanceof BoolQueryBuilder);
  }

  @Override
  public String explain() {
    return getRequest().toString();
  }
}
