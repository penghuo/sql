/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.sql.elasticsearch.aggregation;

import com.amazon.opendistroforelasticsearch.sql.expression.Expression;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ExpressionAggregatorFactory
    extends ValuesSourceAggregatorFactory<ValuesSource.Numeric> {
  private final SearchLookup searchLookup;
  private final Expression expression;

  public ExpressionAggregatorFactory(
      String name,
      ValuesSourceConfig<ValuesSource.Numeric> config,
      QueryShardContext queryShardContext,
      AggregatorFactory parent,
      AggregatorFactories.Builder subFactoriesBuilder,
      Map<String, Object> metaData,
      SearchLookup searchLookup,
      Expression expression)
      throws IOException {
    super(name, config, queryShardContext, parent, subFactoriesBuilder, metaData);
    this.searchLookup = searchLookup;
    this.expression = expression;
  }

  @Override
  protected Aggregator createUnmapped(
      SearchContext searchContext,
      Aggregator parent,
      List<PipelineAggregator> pipelineAggregators,
      Map<String, Object> metaData)
      throws IOException {
    return new ExpressionAvgAggregator(
        name,
        searchContext,
        parent,
        pipelineAggregators,
        metaData,
        searchLookup,
        config.format(),
        expression);
  }

  @Override
  protected Aggregator doCreateInternal(
      ValuesSource.Numeric valuesSource,
      SearchContext searchContext,
      Aggregator parent,
      boolean collectsFromSingleBucket,
      List<PipelineAggregator> pipelineAggregators,
      Map<String, Object> metaData)
      throws IOException {
    return new ExpressionAvgAggregator(
        name,
        searchContext,
        parent,
        pipelineAggregators,
        metaData,
        searchLookup,
        config.format(),
        expression);
  }
}
