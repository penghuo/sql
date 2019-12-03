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

public class CalciteTranslated {

    public org.apache.calcite.linq4j.Enumerable bind(final org.apache.calcite.DataContext root) {
        return ((com.amazon.opendistroforelasticsearch.sql.calcite.ElasticsearchTable.ElasticsearchQueryable) org.apache.calcite.schema.Schemas
                .queryable(
                        root,
                        root.getRootSchema().getSubSchema("elastic"),
                        java.lang.Object[].class,
                        "account")).find(
                java.util.Collections.EMPTY_LIST,
                java.util.Arrays.asList(new org.apache.calcite.util.Pair[]{
                        new org.apache.calcite.util.Pair(
                                "gender",
                                java.lang.String.class),
                        new org.apache.calcite.util.Pair(
                                "c",
                                long.class)}),
                java.util.Arrays.asList(new org.apache.calcite.util.Pair[]{}),
                java.util.Arrays.asList("gender"),
                java.util.Arrays.asList(new org.apache.calcite.util.Pair[]{
                        new org.apache.calcite.util.Pair(
                                "c",
                                "{\"value_count\":{\"field\":\"_id\"}}")}),
                com.google.common.collect.ImmutableMap.of(),
                null,
                null).orderBy(new org.apache.calcite.linq4j.function.Function1() {
                                  public long apply(Object[] v) {
                                      return org.apache.calcite.runtime.SqlFunctions.toLong(v[1]);
                                  }

                                  public Object apply(Object v) {
                                      return apply(
                                              (Object[]) v);
                                  }
                              }
                , org.apache.calcite.linq4j.function.Functions.nullsComparator(false, false));
    }


    public Class getElementType() {
        return java.lang.Object[].class;
    }


}
