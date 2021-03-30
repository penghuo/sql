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

import java.io.IOException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

public class PutEventResponse extends ActionResponse implements ToXContentObject {

  private final RestStatus restStatus;

  public PutEventResponse(RestStatus restStatus) {
    this.restStatus = restStatus;
  }

  public PutEventResponse(StreamInput in) throws IOException {
    super(in);
    restStatus = in.readEnum(RestStatus.class);
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeEnum(restStatus);
  }

  @Override
  public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
    return builder
        .startObject()
        .field("status", restStatus)
        .endObject();
  }
}
