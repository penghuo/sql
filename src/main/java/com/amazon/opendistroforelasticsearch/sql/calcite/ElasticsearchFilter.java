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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Objects;

/**
 * Implementation of a {@link org.apache.calcite.rel.core.Filter}
 * relational expression in Elasticsearch.
 */
public class ElasticsearchFilter extends Filter implements ElasticsearchRel {
  ElasticsearchFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode child,
                      RexNode condition) {
    super(cluster, traitSet, child, condition);
    assert getConvention() == ElasticsearchRel.CONVENTION;
    assert getConvention() == child.getConvention();
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  @Override public Filter copy(RelTraitSet relTraitSet, RelNode input, RexNode condition) {
    return new ElasticsearchFilter(getCluster(), relTraitSet, input, condition);
  }

  @Override public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    ObjectMapper mapper = implementor.elasticsearchTable.mapper;
    PredicateAnalyzerTranslator translator = new PredicateAnalyzerTranslator(mapper);
    try {
      implementor.add(translator.translateMatch(condition));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (PredicateAnalyzer.ExpressionNotAnalyzableException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * New version of translator which uses visitor pattern
   * and allow to process more complex (boolean) predicates.
   */
  static class PredicateAnalyzerTranslator {
    private final ObjectMapper mapper;

    PredicateAnalyzerTranslator(final ObjectMapper mapper) {
      this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    String translateMatch(RexNode condition) throws IOException,
        PredicateAnalyzer.ExpressionNotAnalyzableException {

      StringWriter writer = new StringWriter();
      JsonGenerator generator = mapper.getFactory().createGenerator(writer);
      QueryBuilders.constantScoreQuery(PredicateAnalyzer.analyze(condition)).writeJson(generator);
      generator.flush();
      generator.close();
      return "{\"query\" : " + writer.toString() + "}";
    }
  }

}

// End ElasticsearchFilter.java
