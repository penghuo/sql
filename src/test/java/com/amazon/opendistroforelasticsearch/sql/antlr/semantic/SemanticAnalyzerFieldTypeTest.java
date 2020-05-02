/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.sql.antlr.semantic;

import com.amazon.opendistroforelasticsearch.sql.antlr.OpenDistroSqlAnalyzer;
import com.amazon.opendistroforelasticsearch.sql.antlr.SqlAnalysisConfig;
import com.amazon.opendistroforelasticsearch.sql.esdomain.LocalClusterState;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static com.amazon.opendistroforelasticsearch.sql.util.CheckScriptContents.createParser;
import static com.amazon.opendistroforelasticsearch.sql.util.CheckScriptContents.mockIndexNameExpressionResolver;
import static com.amazon.opendistroforelasticsearch.sql.util.CheckScriptContents.mockSqlSettings;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.allOf;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SemanticAnalyzerFieldTypeTest {
    private OpenDistroSqlAnalyzer analyzer = new OpenDistroSqlAnalyzer(new SqlAnalysisConfig(true, true, 1000));

    private final static String INDEX_ACCOUNT_1 = "account1";
    private final static String INDEX_ACCOUNT_2 = "account2";
    private final static String INDEX_ACCOUNT_ALL = "account*";

    private static String INDEX_ACCOUNT_1_MAPPING = "{\n" +
            "  \"field_mappings\": {\n" +
            "    \"mappings\": {\n" +
            "      \"account1\": {\n" +
            "        \"properties\": {\n" +
            "          \"address\": {\n" +
            "            \"type\": \"text\"\n" +
            "          },\n" +
            "          \"age\": {\n" +
            "            \"type\": \"integer\"\n" +
            "          },\n" +
            "          \"employer\": {\n" +
            "            \"type\": \"text\",\n" +
            "            \"fields\": {\n" +
            "              \"keyword\": {\n" +
            "                \"type\": \"keyword\",\n" +
            "                \"ignore_above\": 256\n" +
            "              }\n" +
            "            }\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    },\n" +
            "    \"settings\": {\n" +
            "      \"index\": {\n" +
            "        \"number_of_shards\": 1,\n" +
            "        \"number_of_replicas\": 0,\n" +
            "        \"version\": {\n" +
            "          \"created\": \"6050399\"\n" +
            "        }\n" +
            "      }\n" +
            "    },    \n" +
            "    \"mapping_version\": \"1\",\n" +
            "    \"settings_version\": \"1\"\n" +
            "  }\n" +
            "}";

    private static String INDEX_ACCOUNT_2_MAPPING = "{\n" +
            "  \"field_mappings\": {\n" +
            "    \"mappings\": {\n" +
            "      \"account2\": {\n" +
            "        \"properties\": {\n" +
            "          \"address\": {\n" +
            "            \"type\": \"text\"\n" +
            "          },\n" +
            "          \"age\": {\n" +
            "            \"type\": \"integer\"\n" +
            "          },\n" +
            "          \"employer\": {\n" +
            "            \"type\": \"nested\",\n" +
            "            \"properties\": {\n" +
            "              \"salary\": {\n" +
            "                \"type\": \"integer\"\n" +
            "              },\n" +
            "              \"depto\": {\n" +
            "                \"type\": \"integer\"\n" +
            "              }\n" +
            "            }\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    },\n" +
            "    \"settings\": {\n" +
            "      \"index\": {\n" +
            "        \"number_of_shards\": 1,\n" +
            "        \"number_of_replicas\": 0,\n" +
            "        \"version\": {\n" +
            "          \"created\": \"6050399\"\n" +
            "        }\n" +
            "      }\n" +
            "    },    \n" +
            "    \"mapping_version\": \"1\",\n" +
            "    \"settings_version\": \"1\"\n" +
            "  }\n" +
            "}";

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setup() throws IOException {
        mockLocalClusterState(new ImmutableMap.Builder<String, ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>>>()
                .put(INDEX_ACCOUNT_1, buildIndexMapping(INDEX_ACCOUNT_1, INDEX_ACCOUNT_1_MAPPING))
                .put(INDEX_ACCOUNT_2, buildIndexMapping(INDEX_ACCOUNT_2, INDEX_ACCOUNT_2_MAPPING))
                .put(INDEX_ACCOUNT_ALL, buildIndexMapping(new ImmutableMap.Builder<String, String>()
                        .put(INDEX_ACCOUNT_1, INDEX_ACCOUNT_1_MAPPING)
                        .put(INDEX_ACCOUNT_2, INDEX_ACCOUNT_2_MAPPING)
                        .build()))
                .build());
    }


    @Test
    public void fieldTypeConflict() {
        expectValidationFailWithErrorMessages("SELECT employer FROM account* WHERE employer = 'bob'");
    }


    protected void validate(String sql) {
        analyzer.analyze(sql, LocalClusterState.state());
    }

    protected void expectValidationFailWithErrorMessages(String query, String... messages) {
        exception.expect(SemanticAnalysisException.class);
        exception.expectMessage(allOf(Arrays.stream(messages).
                map(Matchers::containsString).
                collect(toList())));
        validate(query);
    }

    public static void mockLocalClusterState(Map<String, ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>>> indexMapping) {
        LocalClusterState.state().setClusterService(mockClusterService(indexMapping));
        LocalClusterState.state().setResolver(mockIndexNameExpressionResolver());
        LocalClusterState.state().setSqlSettings(mockSqlSettings());
    }


    public static ClusterService mockClusterService(Map<String, ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>>> indexMapping) {
        ClusterService mockService = mock(ClusterService.class);
        ClusterState mockState = mock(ClusterState.class);
        MetaData mockMetaData = mock(MetaData.class);

        when(mockService.state()).thenReturn(mockState);
        when(mockState.metaData()).thenReturn(mockMetaData);
        try {
            for (Map.Entry<String, ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>>> entry : indexMapping.entrySet()) {
                when(mockMetaData.findMappings(eq(new String[]{entry.getKey()}), any(), any())).thenReturn(entry.getValue());
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return mockService;
    }

    private ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> buildIndexMapping(Map<String, String> indexMapping) {
        try {
            ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> builder = ImmutableOpenMap.builder();
            for (Map.Entry<String, String> entry : indexMapping.entrySet()) {
                builder.put(entry.getKey(), IndexMetaData.fromXContent(createParser(entry.getValue())).getMappings());
            }
            return builder.build();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> buildIndexMapping(String index,
                                                                                                  String mapping) {
        try {
            ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> builder = ImmutableOpenMap.builder();
            builder.put(index, IndexMetaData.fromXContent(createParser(mapping)).getMappings());
            return builder.build();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
