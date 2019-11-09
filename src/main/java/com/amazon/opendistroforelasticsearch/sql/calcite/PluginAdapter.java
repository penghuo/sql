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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.elasticsearch.client.RestClient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PluginAdapter {

    public static Connection createConnection(RestClient client) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        SchemaPlus root = connection.unwrap(CalciteConnection.class).getRootSchema();
        root.add("elastic", new ElasticsearchSchema(client, new ObjectMapper(), null));
        return connection;
    }
}
