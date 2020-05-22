/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.sql.planner.physical;

import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.expression.Expression;
import com.amazon.opendistroforelasticsearch.sql.storage.bindingtuple.BindingTuple;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/**
 * Dedupe operator. Dedupe the input {@link ExprValue} by using the {@link
 * DedupeOperator#dedupeList} The result order follow the input order.
 */
public class DedupeOperator extends PhysicalPlan {
  private final PhysicalPlan input;
  private final List<Expression> dedupeList;
  private final Deduper<List<ExprValue>> deduper;
  private final Integer allowedDuplication;
  private final Boolean keepEmpty;
  private ExprValue next;

  private static final Integer ALL_ONE_DUPLICATION = 1;
  private static final Boolean IGNORE_EMPTY = false;
  private static final Boolean NON_CONSECUTIVE = false;

  private static final Predicate<ExprValue> NULL_OR_MISSING = v -> v.isNull() || v.isMissing();

  public DedupeOperator(PhysicalPlan input, List<Expression> dedupeList) {
    this(input, dedupeList, ALL_ONE_DUPLICATION, IGNORE_EMPTY, NON_CONSECUTIVE);
  }

  public DedupeOperator(
      PhysicalPlan input,
      List<Expression> dedupeList,
      Integer allowedDuplication,
      Boolean keepEmpty,
      Boolean consecutive) {
    this.input = input;
    this.dedupeList = dedupeList;
    this.allowedDuplication = allowedDuplication;
    this.keepEmpty = keepEmpty;
    this.deduper = consecutive ? new ConsecutiveDeduper<>() : new HistoricalDeduper<>();
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitDedupe(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.singletonList(input);
  }

  @Override
  public boolean hasNext() {
    while (input.hasNext()) {
      ExprValue next = input.next();
      if (keep(next)) {
        this.next = next;
        return true;
      }
    }
    return false;
  }

  @Override
  public ExprValue next() {
    return this.next;
  }

  /**
   * Test the {@link ExprValue} should be keep or ignore
   *
   * <p>If any value evaluted by {@link DedupeOperator#dedupeList} is NULL or MISSING, then the *
   * return value is decided by keepEmpty option, default value is ignore.
   *
   * @param value
   * @return true: keep, false: ignore
   */
  public boolean keep(ExprValue value) {
    BindingTuple bindingTuple = value.bindingTuples();
    ImmutableList.Builder<ExprValue> dedupeKeyBuilder = new ImmutableList.Builder<>();
    for (Expression expression : dedupeList) {
      ExprValue exprValue = expression.valueOf(bindingTuple);
      if (NULL_OR_MISSING.test(exprValue)) {
        return keepEmpty;
      }
      dedupeKeyBuilder.add(exprValue);
    }
    List<ExprValue> dedupeKey = dedupeKeyBuilder.build();
    int seenTimes = deduper.seenTimes(dedupeKey);
    return seenTimes <= allowedDuplication;
  }

  /**
   * Return how many times the dedupeKey has been seen before. The side effect is the seen times
   * will add 1 times after calling this function.
   *
   * @param <K> dedupe key
   */
  interface Deduper<K> {

    int seenTimes(K dedupeKey);
  }

  /** The Historical Deduper monitor the duplicated element with all the seen value. */
  static class HistoricalDeduper<K> implements Deduper<K> {
    private final Map<K, Integer> seenMap = new ConcurrentHashMap<>();

    @Override
    public int seenTimes(K dedupeKey) {
      seenMap.putIfAbsent(dedupeKey, 0);
      return seenMap.computeIfPresent(dedupeKey, (k, v) -> v + 1);
    }
  }

  /**
   * The Consecutive Deduper monitor the duplicated element with consecutive seen value. It means
   * only the consecutive duplicated value will be counted.
   */
  static class ConsecutiveDeduper<K> implements Deduper<K> {
    private K lastSeenDedupeKey = null;
    private Integer consecutiveCount = 0;

    @Override
    public int seenTimes(K dedupeKey) {
      if (dedupeKey.equals(lastSeenDedupeKey)) {
        return ++consecutiveCount;
      } else {
        lastSeenDedupeKey = dedupeKey;
        consecutiveCount = 1;
        return consecutiveCount;
      }
    }
  }
}
