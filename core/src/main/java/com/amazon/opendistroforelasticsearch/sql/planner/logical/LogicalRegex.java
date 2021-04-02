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

package com.amazon.opendistroforelasticsearch.sql.planner.logical;

import com.amazon.opendistroforelasticsearch.sql.expression.Expression;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Logical Regex Command.
 */
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalRegex extends LogicalPlan {

  @Getter
  private final Expression expression;

  @Getter
  private final String pattern;

  @Getter
  private final List<String> groups;

  /**
   * Constructor of LogicalEval.
   */
  public LogicalRegex(LogicalPlan child, Expression expression, String pattern) {
    super(Collections.singletonList(child));
    this.expression = expression;
    this.pattern = pattern;
    this.groups = getNamedGroupCandidates(pattern);
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitRegex(this, context);
  }

  private static List<String> getNamedGroupCandidates(String regex) {
    List<String> namedGroups = new ArrayList<>();
    Matcher m = Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>").matcher(regex);
    while (m.find()) {
      namedGroups.add(m.group(1));
    }
    return namedGroups;
  }
}
