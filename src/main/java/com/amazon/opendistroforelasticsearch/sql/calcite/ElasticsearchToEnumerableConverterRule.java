/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */
package com.amazon.opendistroforelasticsearch.sql.calcite;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
 * Rule to convert a relational expression from
 * {@link ElasticsearchRel#CONVENTION} to {@link EnumerableConvention}.
 */
public class ElasticsearchToEnumerableConverterRule extends ConverterRule {
  static final ConverterRule INSTANCE =
      new ElasticsearchToEnumerableConverterRule(RelFactories.LOGICAL_BUILDER);

  /**
   * Creates an ElasticsearchToEnumerableConverterRule.
   *
   * @param relBuilderFactory Builder for relational expressions
   */
  private ElasticsearchToEnumerableConverterRule(
      RelBuilderFactory relBuilderFactory) {
    super(RelNode.class, (Predicate<RelNode>) r -> true,
          ElasticsearchRel.CONVENTION, EnumerableConvention.INSTANCE,
          relBuilderFactory, "ElasticsearchToEnumerableConverterRule");
  }

  @Override public RelNode convert(RelNode relNode) {
    RelTraitSet newTraitSet = relNode.getTraitSet().replace(getOutConvention());
    return new ElasticsearchToEnumerableConverter(relNode.getCluster(), newTraitSet, relNode);
  }
}

// End ElasticsearchToEnumerableConverterRule.java
