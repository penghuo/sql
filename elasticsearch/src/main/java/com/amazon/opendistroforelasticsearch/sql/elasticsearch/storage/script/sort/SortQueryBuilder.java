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

import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.STRING;
import static com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.type.ElasticsearchDataType.ES_TEXT;
import static com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.type.ElasticsearchDataType.ES_TEXT_KEYWORD;
import static com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.script.ExpressionScriptEngine.EXPRESSION_LANG_NAME;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.script.Script.DEFAULT_SCRIPT_TYPE;

import com.amazon.opendistroforelasticsearch.sql.ast.tree.Sort;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.script.ScriptUtils;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.serialization.DefaultExpressionSerializer;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.serialization.ExpressionSerializer;
import com.amazon.opendistroforelasticsearch.sql.expression.Expression;
import com.amazon.opendistroforelasticsearch.sql.expression.ExpressionNodeVisitor;
import com.amazon.opendistroforelasticsearch.sql.expression.FunctionExpression;
import com.amazon.opendistroforelasticsearch.sql.expression.ReferenceExpression;
import com.amazon.opendistroforelasticsearch.sql.expression.conditional.cases.CaseClause;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

/**
 * Builder of {@link SortBuilder}.
 */
public class SortQueryBuilder {

  private Map<Sort.SortOrder, SortOrder> sortOrderMap =
      new ImmutableMap.Builder<Sort.SortOrder, SortOrder>()
          .put(Sort.SortOrder.ASC, SortOrder.ASC)
          .put(Sort.SortOrder.DESC, SortOrder.DESC)
          .build();
  private Map<Sort.NullOrder, String> missingMap =
      new ImmutableMap.Builder<Sort.NullOrder, String>()
          .put(Sort.NullOrder.NULL_FIRST, "_first")
          .put(Sort.NullOrder.NULL_LAST, "_last")
          .build();

  /**
   * Build {@link SortBuilder}.
   *
   * @param expression expression
   * @param option sort option
   * @return SortBuilder.
   */
  public SortBuilder<?> build(Expression expression, Sort.SortOption option) {
    if (expression instanceof ReferenceExpression) {
      return fieldBuild((ReferenceExpression) expression, option);
    } else {
      return scriptBuild(expression, option);
    }
  }

  private FieldSortBuilder fieldBuild(ReferenceExpression ref, Sort.SortOption option) {
    return SortBuilders.fieldSort(ScriptUtils.convertTextToKeyword(ref.getAttr(), ref.type()))
        .order(sortOrderMap.get(option.getSortOrder()))
        .missing(missingMap.get(option.getNullOrder()));
  }

  private ScriptSortBuilder scriptBuild(Expression expression, Sort.SortOption option) {
    return SortBuilders
        .scriptSort(new SortScriptBuilder().build(expression), sortType(expression.type()))
        .order(sortOrderMap.get(option.getSortOrder()));
  }

  /**
   * Mapping the expression type to {@link ScriptSortBuilder.ScriptSortType}.
   *
   * @param type ExprType
   * @return {@link ScriptSortBuilder.ScriptSortType}
   */
  @VisibleForTesting
  public ScriptSortBuilder.ScriptSortType sortType(ExprType type) {
    if (type.equals(STRING) || type.equals(ES_TEXT) || type.equals(ES_TEXT_KEYWORD)) {
      return ScriptSortBuilder.ScriptSortType.STRING;
    } else {
      return ScriptSortBuilder.ScriptSortType.NUMBER;
    }
  }

  private static class SortScriptBuilder extends ExpressionNodeVisitor<Script, Object> {

    private final ExpressionSerializer serializer = new DefaultExpressionSerializer();

    public Script build(Expression expression) {
      return expression.accept(this, null);
    }

    @Override
    public Script visitFunction(FunctionExpression node, Object context) {
      return new Script(DEFAULT_SCRIPT_TYPE, EXPRESSION_LANG_NAME, serializer.serialize(node),
          emptyMap());
    }

    @Override
    public Script visitCase(CaseClause node, Object context) {
      return new Script(DEFAULT_SCRIPT_TYPE, EXPRESSION_LANG_NAME, serializer.serialize(node),
          emptyMap());
    }
  }


}
