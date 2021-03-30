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
import java.util.Map;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;

public class PutEventRequest extends ActionRequest {

  public String getIndex() {
    return index;
  }

  public String getType() {
    return type;
  }

  public String getBucket() {
    return bucket;
  }

  public String getObject() {
    return object;
  }

  public Map<String, Object> getTags() {
    return tags;
  }

  private String index;
  private String type;
  private String bucket;
  private String object;
  private Map<String, Object> tags;

  public PutEventRequest(String index, String type, String bucket, String object,
                         Map<String, Object> tags) {
    this.index = index;
    this.type = type;
    this.bucket = bucket;
    this.object = object;
    this.tags = tags;
  }

  public PutEventRequest(StreamInput in) throws IOException {
    super(in);
    index = in.readString();
    type = in.readString();
    bucket = in.readString();
    object = in.readString();
    tags = in.readMap();
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }

  @Override
  public String toString() {
    return "PutEventRequest{" +
        "index='" + index + '\'' +
        ", type='" + type + '\'' +
        ", bucket='" + bucket + '\'' +
        ", object='" + object + '\'' +
        ", tags=" + tags +
        '}';
  }
}
