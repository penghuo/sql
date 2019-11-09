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

import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Internal constants referenced in this package.
 */
interface ElasticsearchConstants {

  String INDEX = "_index";
  String TYPE = "_type";
  String FIELDS = "fields";
  String SOURCE_PAINLESS = "params._source";
  String SOURCE_GROOVY = "_source";

  /**
   * Attribute which uniquely identifies a document (ID)
   * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-id-field.html">ID Field</a>
   */
  String ID = "_id";
  String UID = "_uid";

  Set<String> META_COLUMNS = ImmutableSet.of(UID, ID, TYPE, INDEX);

  /**
   * Detects {@code select * from elastic} types of field name (select star).
   * @param name name of the field
   * @return {@code true} if this field represents whole raw, {@code false} otherwise
   */
  static boolean isSelectAll(String name) {
    return "_MAP".equals(name);
  }

}

// End ElasticsearchConstants.java
