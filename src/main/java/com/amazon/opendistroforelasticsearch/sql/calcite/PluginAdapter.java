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
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.Closer;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class PluginAdapter {

    public static RestClient createRestClient() {
        return RestClient.builder(
                new HttpHost("localhost", 9200, "http"),
                new HttpHost("localhost", 9201, "http")).build();
    }

    public static Connection createConnection(RestClient client) {
        try {
            Connection connection = DriverManager.getConnection("jdbc:calcite:");
            SchemaPlus root = connection.unwrap(CalciteConnection.class).getRootSchema();
            root.add("elastic", new ElasticsearchSchema(client, new ObjectMapper(), null));
            return connection;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static String calcite(String sql) {
        Connection connection = createConnection(createRestClient());
        boolean materializationsEnabled = false;
        int limit = 0;
        String result = "";
        try (Closer closer = new Closer()) {
            if (connection.isWrapperFor(CalciteConnection.class)) {
                final CalciteConnection calciteConnection =
                        connection.unwrap(CalciteConnection.class);
                final Properties properties = calciteConnection.getProperties();
                properties.setProperty(
                        CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(),
                        Boolean.toString(materializationsEnabled));
                properties.setProperty(
                        CalciteConnectionProperty.CREATE_MATERIALIZATIONS.camelName(),
                        Boolean.toString(materializationsEnabled));
                //                // case sensitive = false
                //                properties.setProperty(
                //                        CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
                //                        Boolean.toString(false));
                properties.setProperty(
                        CalciteConnectionProperty.LEX.camelName(),
                        Lex.MYSQL.name());
                properties.setProperty(
                        CalciteConnectionProperty.CONFORMANCE.camelName(),
                        SqlConformanceEnum.LENIENT.name());

                if (!properties
                        .containsKey(CalciteConnectionProperty.TIME_ZONE.camelName())) {
                    // Do not override id some test has already set this property.
                    properties.setProperty(
                            CalciteConnectionProperty.TIME_ZONE.camelName(),
                            DateTimeUtils.UTC_ZONE.getID());
                }
            }

            Statement statement = connection.createStatement();
            statement.setMaxRows(limit <= 0 ? limit : Math.max(limit, 1));
            ResultSet resultSet = null;
            try {
                resultSet = statement.executeQuery(sql);
            } catch (Exception | Error e) {
                throw e;
            }
            if (resultSet != null) {
                result = new ResultSetFormatter().resultSet(resultSet).string();
                resultSet.close();
            }
            statement.close();
            connection.close();
            return result;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
