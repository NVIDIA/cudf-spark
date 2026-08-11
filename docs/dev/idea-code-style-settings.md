---
layout: page
title: IDEA Code Style Settings
nav_order: 5
parent: Developer Overview
---
```xml
<code_scheme name="Default" version="173">
  <option name="SOFT_MARGINS" value="100" />
  <JavaCodeStyleSettings>
    <option name="CLASS_COUNT_TO_USE_IMPORT_ON_DEMAND" value="10" />
    <option name="NAMES_COUNT_TO_USE_IMPORT_ON_DEMAND" value="10" />
  </JavaCodeStyleSettings>
  <ScalaCodeStyleSettings>
    <option name="classCountToUseImportOnDemand" value="15" />
    <option name="importLayout">
      <array>
        <option value="java" />
        <option value="_______ blank line _______" />
        <option value="scala" />
        <option value="_______ blank line _______" />
        <option value="all other imports" />
        <option value="_______ blank line _______" />
        <option value="org.apache.spark" />
      </array>
    </option>
    <option name="sortAsScalastyle" value="true" />
    <option name="USE_ALTERNATE_CONTINUATION_INDENT_FOR_PARAMS" value="true" />
  </ScalaCodeStyleSettings>
  <codeStyleSettings language="JAVA">
    <option name="INDENT_CASE_FROM_SWITCH" value="false" />
    <option name="ALIGN_MULTILINE_PARAMETERS" value="false" />
    <indentOptions>
      <option name="INDENT_SIZE" value="2" />
      <option name="CONTINUATION_INDENT_SIZE" value="4" />
      <option name="TAB_SIZE" value="2" />
    </indentOptions>
  </codeStyleSettings>
  <codeStyleSettings language="Scala">
    <option name="ALIGN_MULTILINE_PARAMETERS" value="false" />
    <indentOptions>
      <option name="CONTINUATION_INDENT_SIZE" value="4" />
    </indentOptions>
  </codeStyleSettings>
</code_scheme>
```
