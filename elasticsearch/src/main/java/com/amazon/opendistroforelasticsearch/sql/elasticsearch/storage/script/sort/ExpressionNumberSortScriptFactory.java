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

package com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.script.sort;

import com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.script.aggregation.ExpressionAggregationScriptLeafFactory;
import com.amazon.opendistroforelasticsearch.sql.expression.Expression;
import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import lombok.EqualsAndHashCode;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.script.NumberSortScript;
import org.elasticsearch.search.lookup.SearchLookup;

/**
 * Todo.
 */
@EqualsAndHashCode
public class ExpressionNumberSortScriptFactory implements NumberSortScript.Factory {

  private final Expression expression;

  public ExpressionNumberSortScriptFactory(Expression expression) {
    this.expression = expression;
  }

  @Override
  public NumberSortScript.LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup) {
    return new ExpressionNumberSortScriptLeafFactory(expression, params, lookup);
  }

  @Override
  public boolean isResultDeterministic() {
    return false;
  }

  /**
   * Expression script leaf factory that produces script executor for each leaf.
   */
  @VisibleForTesting
  public static class ExpressionNumberSortScriptLeafFactory
      implements NumberSortScript.LeafFactory {

    /**
     * Expression to execute.
     */
    private final Expression expression;

    /**
     * Parameters for the expression.
     */
    private final Map<String, Object> params;

    /**
     * Document lookup that returns doc values.
     */
    private final SearchLookup lookup;

    /**
     * Todo.
     */
    public ExpressionNumberSortScriptLeafFactory(Expression expression,
                                                 Map<String, Object> params,
                                                 SearchLookup lookup) {
      this.expression = expression;
      this.params = params;
      this.lookup = lookup;
    }

    @Override
    public NumberSortScript newInstance(LeafReaderContext ctx) {
      return new ExpressionNumberSortScript(expression, lookup, ctx, params);
    }

    @Override
    public boolean needs_score() {
      return false;
    }

  }
}
