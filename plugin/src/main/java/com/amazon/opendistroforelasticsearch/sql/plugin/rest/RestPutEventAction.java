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
import com.amazon.opendistroforelasticsearch.sql.plugin.transport.PutEventRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

public class RestPutEventAction extends BaseRestHandler {
  private static final Logger log = LogManager.getLogger(RestPutEventAction.class);

  private static final String PUT_EVENT_ACTION = "put_event_action";

  private static ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public String getName() {
    return PUT_EVENT_ACTION;
  }

  @Override
  public List<Route> routes() {
    return Arrays.asList(
        new Route(RestRequest.Method.POST, "/_opensearch/log-stream/")
    );
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client)
      throws IOException {
    return channel -> client.execute(PutEventAction.INSTANCE, buildRequest(request),
        new RestToXContentListener<>(channel));
  }

  private PutEventRequest buildRequest(RestRequest request) throws JsonProcessingException {
    String content = request.content().utf8ToString();

    final JsonNode jsonNode = objectMapper.readTree(content);

    final Iterator<String> iterator = jsonNode.fieldNames();
    String metaType = jsonNode.at("/meta/type").asText();
    String metaBucket = jsonNode.at("/meta/bucket").asText();
    String metaObject = jsonNode.at("/meta/object").asText();
    String index = jsonNode.at("/meta/index").asText();
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
    return new PutEventRequest(index, metaType, metaBucket, metaObject, tags);
  }

}
