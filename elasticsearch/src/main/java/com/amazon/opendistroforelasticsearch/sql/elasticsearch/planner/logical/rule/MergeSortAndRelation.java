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

package com.amazon.opendistroforelasticsearch.sql.elasticsearch.planner.logical.rule;

import static com.amazon.opendistroforelasticsearch.sql.planner.optimizer.pattern.Patterns.source;
import static com.facebook.presto.matching.Pattern.typeOf;

import com.amazon.opendistroforelasticsearch.sql.elasticsearch.planner.logical.ElasticsearchLogicalIndexScan;
import com.amazon.opendistroforelasticsearch.sql.expression.ReferenceExpression;
import com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalPlan;
import com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalRelation;
import com.amazon.opendistroforelasticsearch.sql.planner.logical.LogicalSort;
import com.amazon.opendistroforelasticsearch.sql.planner.optimizer.Rule;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;

/**
 * Merge Sort with Relation only when Sort by fields.
 */
public class MergeSortAndRelation implements Rule<LogicalSort> {

  private final Capture<LogicalRelation> relationCapture;
  private final Pattern<LogicalSort> pattern;

  /**
   * Constructor of MergeSortAndRelation.
   */
  public MergeSortAndRelation() {
    this.relationCapture = Capture.newCapture();
    this.pattern =
        typeOf(LogicalSort.class).matching(this::sortByFieldsOnly)
            .with(source()
                .matching(typeOf(LogicalRelation.class).capturedAs(relationCapture))
            );
  }

  @Override
  public Pattern<LogicalSort> pattern() {
    return pattern;
  }

  @Override
  public LogicalPlan apply(LogicalSort sort,
                           Captures captures) {
    LogicalRelation relation = captures.get(relationCapture);
    return ElasticsearchLogicalIndexScan
        .builder()
        .relationName(relation.getRelationName())
        .sortList(sort.getSortList())
        .build();
  }

  private boolean sortByFieldsOnly(LogicalSort logicalSort) {
    return logicalSort.getSortList().stream()
        .map(sort -> sort.getRight() instanceof ReferenceExpression)
        .reduce(true, (l, r) -> l && r);
  }
}
