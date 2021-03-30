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

package com.amazon.opendistroforelasticsearch.sql.plugin.transport;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class PutEventTransportAction extends HandledTransportAction<PutEventRequest, PutEventResponse> {

  private static final Logger log = LogManager.getLogger(PutEventTransportAction.class);

  private final Client client;
  private final ClusterService clusterService;
  private final NamedXContentRegistry xContentRegistry;

  @Inject
  public PutEventTransportAction(
      TransportService transportService,
      ActionFilters actionFilters,
      Client client,
      ClusterService clusterService,
      Settings settings,
      NamedXContentRegistry xContentRegistry
  ) {
    super(PutEventAction.NAME, transportService, actionFilters, PutEventRequest::new);
    this.client = client;
    this.clusterService = clusterService;
    this.xContentRegistry = xContentRegistry;
  }

  @Override
  protected void doExecute(Task task, PutEventRequest request,
                           ActionListener<PutEventResponse> listener) {
      client.index(indexRequest(request), new ActionListener<IndexResponse>() {
        @Override
        public void onResponse(IndexResponse indexResponse) {
          listener.onResponse(new PutEventResponse(RestStatus.OK));
        }

        @Override
        public void onFailure(Exception e) {
          log.error("index event {} failed", request, e);
          listener.onResponse(new PutEventResponse(RestStatus.BAD_REQUEST));
        }
      });
  }

  private IndexRequest indexRequest(PutEventRequest request) {
    Map<String, Object> metaInfo = new HashMap<>();
    Map<String, Object> content = new HashMap<>();

    metaInfo.put("type", "s3");
    metaInfo.put("bucket", request.getBucket());
    metaInfo.put("object", request.getObject());
    content.putAll(request.getTags());
    content.put("meta", metaInfo);
    final IndexRequest indexRequest = new IndexRequest(request.getIndex())
        .source(content);

    log.info("indexing {}", content);
    return indexRequest;
  }
}
