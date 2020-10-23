/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.sql.benchmark.utils.query.elasticsearch;

import com.amazon.opendistroforelasticsearch.sql.benchmark.utils.CommandExecution;
import com.amazon.opendistroforelasticsearch.sql.benchmark.utils.query.QueryRunner;

/**
 * Query runner for Elasticsearch databases.
 */
public class ElasticsearchQueryRunner extends QueryRunner {

  /**
   * Function to run queries against Elasticsearch database.
   *
   * @param query Query to run against Elasticsearch database.
   */
  @Override
  public void runQuery(final String query) throws Exception {
    CommandExecution.executeCommand(
        "curl -XPOST https://localhost:9200/_opendistro/_sql -u admin:admin --insecure "
            + "-k -H 'Content-Type: application/json' -d '{\"query\": \"" + query + "\"}'");
  }
}
