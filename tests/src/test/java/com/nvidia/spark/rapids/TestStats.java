/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nvidia.spark.rapids;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

/** Only use in UT Env. It's not thread safe. */
public class TestStats {
  private static final String HEADER_FORMAT = "<tr><th>%s</th><th colspan=5>%s</th></tr>";
  private static final String ROW_FORMAT =
      "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>";

  private static boolean UT_ENV = false;
  private static final Map<String, CaseInfo> caseInfos = new HashMap<>();
  private static String currentCase;
  public static int offloadRapidsUnitNumber = 0;
  public static int testUnitNumber = 0;

  // use the rapids backend to execute the query
  public static boolean offloadRapids = true;
  public static int suiteTestNumber = 0;
  public static int offloadRapidsTestNumber = 0;

  public static void beginStatistic() {
    UT_ENV = true;
  }

  public static void reset() {
    offloadRapids = false;
    suiteTestNumber = 0;
    offloadRapidsTestNumber = 0;
    testUnitNumber = 0;
    offloadRapidsUnitNumber = 0;
    resetCase();
    caseInfos.clear();
  }

  private static int totalSuiteTestNumber = 0;
  public static int totalOffloadRapidsTestNumber = 0;

  public static int totalTestUnitNumber = 0;
  public static int totalOffloadRapidsCaseNumber = 0;

  public static void printMarkdown(String suitName) {
    if (!UT_ENV) {
      return;
    }

    String title = "print_markdown_" + suitName;

    String info =
        "Case Count: %d, OffloadRapids Case Count: %d, "
            + "Unit Count %d, OffloadRapids Unit Count %d";

    System.out.println(
        String.format(
            HEADER_FORMAT,
            title,
            String.format(
                info,
                TestStats.suiteTestNumber,
                TestStats.offloadRapidsTestNumber,
                TestStats.testUnitNumber,
                TestStats.offloadRapidsUnitNumber)));

    caseInfos.forEach(
        (key, value) ->
            System.out.println(
                String.format(
                    ROW_FORMAT,
                    title,
                    key,
                    value.status,
                    value.type,
                    String.join("<br/>", value.fallbackExpressionName),
                    String.join("<br/>", value.fallbackClassName))));

    totalSuiteTestNumber += suiteTestNumber;
    totalOffloadRapidsTestNumber += offloadRapidsTestNumber;
    totalTestUnitNumber += testUnitNumber;
    totalOffloadRapidsCaseNumber += offloadRapidsUnitNumber;
    System.out.println(
        "total_markdown_ totalCaseNum:"
            + totalSuiteTestNumber
            + " offloadRapids: "
            + totalOffloadRapidsTestNumber
            + " total unit: "
            + totalTestUnitNumber
            + " offload unit: "
            + totalOffloadRapidsCaseNumber);
  }

  public static void addFallBackClassName(String className) {
    if (!UT_ENV) {
      return;
    }

    if (caseInfos.containsKey(currentCase) && !caseInfos.get(currentCase).stack.isEmpty()) {
      CaseInfo info = caseInfos.get(currentCase);
      caseInfos.get(currentCase).fallbackExpressionName.add(info.stack.pop());
      caseInfos.get(currentCase).fallbackClassName.add(className);
    }
  }

  public static void addFallBackCase() {
    if (!UT_ENV) {
      return;
    }

    if (caseInfos.containsKey(currentCase)) {
      caseInfos.get(currentCase).type = "fallback";
    }
  }

  public static void addExpressionClassName(String className) {
    if (!UT_ENV) {
      return;
    }

    if (caseInfos.containsKey(currentCase)) {
      CaseInfo info = caseInfos.get(currentCase);
      info.stack.add(className);
    }
  }

  public static Set<String> getFallBackClassName() {
    if (!UT_ENV) {
      return Collections.emptySet();
    }

    if (caseInfos.containsKey(currentCase)) {
      return Collections.unmodifiableSet(caseInfos.get(currentCase).fallbackExpressionName);
    }

    return Collections.emptySet();
  }

  public static void addIgnoreCaseName(String caseName) {
    if (!UT_ENV) {
      return;
    }

    if (caseInfos.containsKey(caseName)) {
      caseInfos.get(caseName).type = "fatal";
    }
  }

  public static void resetCase() {
    if (!UT_ENV) {
      return;
    }

    if (caseInfos.containsKey(currentCase)) {
      caseInfos.get(currentCase).stack.clear();
    }
    currentCase = "";
  }

  public static void startCase(String caseName) {
    if (!UT_ENV) {
      return;
    }

    caseInfos.putIfAbsent(caseName, new CaseInfo());
    currentCase = caseName;
  }

  public static void endCase(boolean status) {
    if (!UT_ENV) {
      return;
    }

    if (caseInfos.containsKey(currentCase)) {
      caseInfos.get(currentCase).status = status ? "success" : "error";
    }

    resetCase();
  }
}

class CaseInfo {
  final Stack<String> stack = new Stack<>();
  Set<String> fallbackExpressionName = new HashSet<>();
  Set<String> fallbackClassName = new HashSet<>();
  String type = "";
  String status = "";
}
