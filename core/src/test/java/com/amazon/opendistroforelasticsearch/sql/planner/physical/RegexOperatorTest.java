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

package com.amazon.opendistroforelasticsearch.sql.planner.physical;

import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

public class RegexOperatorTest {

  private final static String regex =
      "\\b(?<city>[A-Za-z\\s]+),\\s(?<state>[A-Z]{2,2}):\\s(?<areaCode>[0-9]{3,3})\\b";
  private final static Pattern pattern = Pattern.compile(regex);

  @Test
  public void test() {
    String input = "Baytown, TX: 281";
    final Set<String> groups = getNamedGroupCandidates(regex);
    Matcher matcher = pattern.matcher(input);
    int i = 0;
//    while (matcher.find()) {
//      System.out.println("------------");
//      System.out.println(i++);
//      for (String group : groups) {
//        System.out.println("group " + group + " matching " + matcher.group(group));
//      }
//    }
    System.out.println("------------");
    if (matcher.matches()) {
      for (String group : groups) {
        System.out.println("group " + group + " matching " + matcher.group(group));
      }
    } else {
      System.out.println("not matched");
    }

    System.out.println("done");
  }

  private static Set<String> getNamedGroupCandidates(String regex) {
    Set<String> namedGroups = new TreeSet<String>();

    Matcher m = Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>").matcher(regex);

    while (m.find()) {
      namedGroups.add(m.group(1));
    }

    return namedGroups;
  }

  @Test
  public void test2() {
    String input = "2021-03-01 22:00:04,140 activities [MILESTONE] pid(3847):thr(139665677649664)"
        + ": elasticsearch: swift-us-east-1-prod:613787477748:amznconnect-smesandbox-es:1 Calling ES:ClusterState with {'index': 'yptbs_random_index_that_may_not_exist_xprbs'}";
    Pattern pattern = Pattern.compile(".+pid\\(\\d+\\):thr\\(\\d+\\): (?<message>.+)");
    final Matcher matcher = pattern.matcher(input);

    if (matcher.matches()) {
      System.out.println(matcher.group("message"));
    } else {
      System.out.println("not match");
    }
  }

}
