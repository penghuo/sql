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

package com.amazon.opendistroforelasticsearch.sql.elasticsearch.s3;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

class S3ScanTest {

  @Test
  public void testS3Scan() {
    final ImmutableList<Pair<String, String>> list =
        ImmutableList.of(Pair.of("log.maximus", "poc/elb/1.gz"),
            Pair.of("log.maximus", "poc/elb/2.gz"),
            Pair.of("log.maximus", "poc/elb/3.gz"));
    final S3Scan s3Scan = new S3Scan(list);
    s3Scan.open();
    s3Scan.forEachRemaining(value -> System.out.println(value));
  }
}