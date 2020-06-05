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
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

public class ExpressionAggregationBuilder
    extends ValuesSourceAggregationBuilder.LeafOnly<ValuesSource.Numeric, AvgAggregationBuilder> {
  public static final String NAME = "expression_metric";
  private final Expression experssion;

  public ExpressionAggregationBuilder(String name, Expression experssion) {
    super(name, CoreValuesSourceType.NUMERIC, ValueType.NUMERIC);
    this.experssion = experssion;
  }

  @Override
  protected void innerWriteTo(StreamOutput out) throws IOException {}

  @Override
  protected ValuesSourceAggregatorFactory<ValuesSource.Numeric> innerBuild(
      QueryShardContext queryShardContext,
      ValuesSourceConfig<ValuesSource.Numeric> config,
      AggregatorFactory parent,
      AggregatorFactories.Builder subFactoriesBuilder)
      throws IOException {
    return new ExpressionAggregatorFactory(
        name,
        config,
        queryShardContext,
        parent,
        subFactoriesBuilder,
        metaData,
        queryShardContext.lookup(),
        experssion);
  }

  @Override
  protected XContentBuilder doXContentBody(XContentBuilder builder, Params params)
      throws IOException {
    return null;
  }

  @Override
  protected AggregationBuilder shallowCopy(
      AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metaData) {
    return null;
  }

  @Override
  public String getType() {
    return NAME;
  }
}
