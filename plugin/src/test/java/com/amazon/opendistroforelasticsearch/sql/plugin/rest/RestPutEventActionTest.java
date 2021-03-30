/*
 *     Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License").
 *     You may not use this file except in compliance with the License.
 *     A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *     or in the "license" file accompanying this file. This file is distributed
 *     on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *     express or implied. See the License for the specific language governing
 *     permissions and limitations under the License.
 *
 */

package com.amazon.opendistroforelasticsearch.sql.plugin.rest;

import com.amazon.opendistroforelasticsearch.sql.plugin.transport.PutEventAction;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import junit.framework.TestCase;
import org.junit.Test;

public class RestPutEventActionTest extends TestCase {
  @Test
  public void test() throws JsonProcessingException {
    ObjectMapper om = new ObjectMapper();
    String json = "";
    final JsonNode jsonNode = om.readTree(json);

    final Iterator<String> iterator = jsonNode.fieldNames();
    String index = jsonNode.get("/meta/index").asText();
    String metaType = jsonNode.get("/meta/type").asText();
    String metaBucket = jsonNode.get("/meta/bucket").asText();
    String metaObject = jsonNode.get("/meta/type").asText();
    Map<String, Object> tags = new HashMap<>();
    while (iterator.hasNext()) {
      final String fieldName = iterator.next();
      if (!fieldName.equalsIgnoreCase("meta")) {
        final JsonNode node = jsonNode.get(fieldName);

        if (node.isNumber()) {
          tags.put(fieldName, node.intValue());
        } else {
          tags.put(fieldName, node.asText());
        }
      }
    }

    System.out.println(index);
    System.out.println(tags);
  }

}