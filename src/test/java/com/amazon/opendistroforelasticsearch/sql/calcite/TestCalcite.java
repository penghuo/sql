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

import org.junit.Test;

import static com.amazon.opendistroforelasticsearch.sql.calcite.PluginAdapter.calcite;

public class TestCalcite {

    @Test
    public void sf() {
        System.out.println(calcite("SELECT age " +
                                   "FROM elastic.account"));
    }

    @Test
    public void sfw() {
        System.out.println(calcite("SELECT count(age) " +
                                   "FROM elastic.account " +
                                   "WHERE age = 32"));
    }

    @Test
    public void sfwa() {
        System.out.println(calcite("SELECT gender, count(age) " +
                                   "FROM elastic.account " +
                                   "WHERE age = 32 " +
                                   "GROUP BY gender"));
    }

    @Test
    public void sfwa1() {
        System.out.println(calcite("SELECT gender, count(age) " +
                                   "FROM elastic.account " +
                                   "WHERE age = 32 " +
                                   "GROUP BY 1"));
    }

    @Test
    public void sfwas() {
        System.out.println(calcite("SELECT gender, count(*) as c " +
                                   "FROM elastic.account " +
                                   "GROUP BY gender " +
                                   "ORDER BY c"));
    }

    @Test
    public void join() {
        System.out.println(calcite("SELECT A.age, B.age " +
                                   "FROM elastic.account as A " +
                                   "JOIN elastic.account as B " +
                                   "ON A.age = B.age"));
    }
}
