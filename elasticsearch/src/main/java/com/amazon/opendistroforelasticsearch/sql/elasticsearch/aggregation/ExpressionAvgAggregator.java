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

import com.amazon.opendistroforelasticsearch.sql.data.model.ExprNullValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprType;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValueUtils;
import com.amazon.opendistroforelasticsearch.sql.expression.Expression;
import com.amazon.opendistroforelasticsearch.sql.expression.FunctionExpression;
import com.amazon.opendistroforelasticsearch.sql.expression.ReferenceExpression;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class ExpressionAvgAggregator extends MetricsAggregator {
  private final SearchLookup searchLookup;
  private final DocValueFormat format;
  /** Expression in Aggregation Function, e.g. max(log(x)), expression <- log(x) */
  private final Expression expression;
  private final AvgState avgState = new AvgState();

  LongArray counts;
  DoubleArray sums;

  public ExpressionAvgAggregator(
      String name,
      SearchContext context,
      Aggregator parent,
      List<PipelineAggregator> pipelineAggregators,
      Map<String, Object> metaData,
      SearchLookup searchLookup,
      DocValueFormat format,
      Expression expression)
      throws IOException {
    super(name, context, parent, pipelineAggregators, metaData);
    this.searchLookup = searchLookup;
    this.format = format;
    this.expression = expression;

  }

  @Override
  protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub)
      throws IOException {
    LeafSearchLookup leafSearchLookup = searchLookup.getLeafSearchLookup(ctx);

    return new LeafBucketCollectorBase(sub, avgState) {
      @Override
      public void collect(int doc, long bucket) throws IOException {
        Set<String> fieldNames = extractInputFieldNames();
        Map<String, Object> values =
            extractFieldNameAndValues(fieldNames, () -> leafSearchLookup.doc());
        ExprValue value = evaluateExpression(values);
        avgState.count++;
        avgState.total += ExprValueUtils.getDoubleValue(value);
      }
    };
  }

  @Override
  public InternalAggregation buildAggregation(long bucket) throws IOException {
//    return new InternalAvg(
//        name, sums.get(bucket), counts.get(bucket), format, pipelineAggregators(), metaData());
      return new InternalAvg(
              name, avgState.total, avgState.count, format, pipelineAggregators(), metaData());
  }

  @Override
  public InternalAggregation buildEmptyAggregation() {
    return null;
  }

  private Set<String> extractInputFieldNames() {
    Set<String> fieldNames = new HashSet<>();
    doExtractInputFieldNames(expression, fieldNames);
    return fieldNames;
  }

  private void doExtractInputFieldNames(Expression expr, Set<String> fieldNames) {
    if (expr instanceof FunctionExpression) { // Assume only function input arguments is recursive
      FunctionExpression func = (FunctionExpression) expr;
      func.getArguments().forEach(argExpr -> doExtractInputFieldNames(argExpr, fieldNames));
    } else if (expr instanceof ReferenceExpression) {
      ReferenceExpression ref = (ReferenceExpression) expr;
      fieldNames.add(ref.getAttr());
    }
  }

  private Map<String, Object> extractFieldNameAndValues(
      Set<String> fieldNames, Supplier<Map<String, ScriptDocValues<?>>> getDoc) {
    Map<String, Object> values = new HashMap<>();
    for (String fieldName : fieldNames) {
      ScriptDocValues<?> value = extractFieldValue(fieldName, getDoc);
      if (value != null && !value.isEmpty()) {
        values.put(fieldName, value.get(0));
      }
    }
    return values;
  }

  private ScriptDocValues<?> extractFieldValue(
      String fieldName, Supplier<Map<String, ScriptDocValues<?>>> getDoc) {
    Map<String, ScriptDocValues<?>> doc = getDoc.get();
    String keyword = fieldName + ".keyword";

    ScriptDocValues<?> value = null;
    if (doc.containsKey(keyword)) {
      value = doc.get(keyword);
    } else if (doc.containsKey(fieldName)) {
      value = doc.get(fieldName);
    }
    return value;
  }

  private ExprValue evaluateExpression(Map<String, Object> values) {
    ExprValue tupleValue = ExprValueUtils.tupleValue(values);
    ExprValue result = expression.valueOf(tupleValue.bindingTuples());

    if (result.type() != ExprType.BOOLEAN) {
      throw new IllegalStateException("Expression has wrong result type: " + result);
    }
    return result;
  }

  protected class AvgState {
    private int count;
    private double total;
    private boolean isNullResult = false;

    public AvgState() {
      this.count = 0;
      this.total = 0d;
    }

    public ExprValue result() {
      return isNullResult ? ExprNullValue.of() : ExprValueUtils.doubleValue(total / count);
    }
  }
}
