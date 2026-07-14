/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION.
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
package com.nvidia.spark.rapids

import java.nio.charset.Charset
import java.util.EnumSet
import java.util.regex.{Pattern, PatternSyntaxException}

import scala.collection.mutable.{HashSet, ListBuffer}
import scala.util.{Random, Try}

import ai.rapids.cudf.{CaptureGroups, ColumnVector, CudfException, RegexFlag, RegexProgram}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RegexParser.toReadableString
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.rapids.GpuRegExpUtils
import org.apache.spark.sql.types.DataTypes

class RegularExpressionTranspilerSuite extends AnyFunSuite {

  test("transpiler detects invalid cuDF patterns that cuDF now supports") {
    // these patterns compile in cuDF since https://github.com/rapidsai/cudf/pull/11654 was merged
    // but we still reject them because the behavior is not consistent with Java

    // The test "AST fuzz test - regexp_replace" hangs if we stop rejecting these patterns
    for (pattern <- Seq("\t*|a", "(\t*|a)Dc$1", "\n[^\r\n]x*|^3x")) {
      assertUnsupported(pattern, RegexReplaceMode,
        "cuDF does not support replace or split with zero-length repetition on one side of a" +
        " choice")
    }

    //The test "AST fuzz test - regexp_find" fails if we stop rejecting these patterns.
    for (pattern <- Seq("$|$[^\n]2]}|B")) {
      assertUnsupported(pattern, RegexFindMode,
        "End of line/string anchor is not supported in this context")
    }

  }

  test("transpiler detects invalid cuDF patterns") {
    // The purpose of this test is to document some examples of valid Java regular expressions
    // that fail to compile in cuDF and to check that the transpiler detects these correctly.
    // Many (but not all) of the patterns here are odd edge cases found during testing with
    // random inputs.
    val cudfInvalidPatterns = Seq(
      "a*+",
      "(?d)"
    )
    // data is not relevant because we are checking for compilation errors
    val inputs = Seq("a")
    for (pattern <- cudfInvalidPatterns) {
      // check that this is valid in Java
      Pattern.compile(pattern)
      Seq(RegexFindMode, RegexReplaceMode).foreach { mode =>
        try {
          if (mode == RegexReplaceMode) {
            gpuReplace(pattern, REPLACE_STRING, inputs)
          } else {
            gpuContains(pattern, inputs)
          }
          fail(s"cuDF unexpectedly compiled expression: $pattern")
        } catch {
          case e: CudfException =>
            // expected, now make sure that the transpiler can detect this
            try {
              transpile(pattern, mode)
              fail(
                s"transpiler failed to detect invalid cuDF pattern (mode=$mode): $pattern", e)
            } catch {
              case _: RegexUnsupportedException =>
                // expected
            }
        }
      }
    }
  }

  test("Detect unsupported combinations of line anchors and \\W and \\D") {
    val patterns = Seq("\\W\\Z\\D", "\\W$", "$\\D")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, RegexFindMode,
        "End of line/string anchor is not supported in this context")
    )
  }

  test("choice with repetition - regexp_find") {
    val patterns = Seq("b?|a", "b*|^\t", "b+|^\t", "a|b+", "a+|b+", "a{2,3}|b+", "a*|b+",
      "b*?|^\t", "b+?|^\t", "a|b+?", "a+?|b+?", "a{2,3}|b+?", "a*?|b+?", "[cat]{3}|dog")
    assertCpuGpuMatchesRegexpFind(patterns, Seq("aaa", "bb", "a\tb", "aaaabbbb", "a\tb\ta\tb"))
  }

  test("choice with repetition - regexp_replace") {
    val patterns = Seq("b+|^\t", "a|b+", "a+|b+", "a{2,3}|b+", "[cat]{3}|dog")
    assertCpuGpuMatchesRegexpReplace(patterns, Seq("aaa", "bb", "a\tb", "aaaabbbb", "a\tb\ta\tb"))
  }

  test("zero-length repetition near line anchor  - regexp_find") {
    val patterns = Seq("\\00*[D$3]$", "\\00*[D$3]\\Z", "^([a-z]*)([0-9]*)([a-z]*)$")
    val inputs = Seq("abcd", "abc012abc", "999abb", "\\00D", "D", "D\n", "\\00D\n\r")
    assertCpuGpuMatchesRegexpFind(patterns, inputs)
  }

  test("zero-length repetition near line anchor  - regexp_replace") {
    val patterns = Seq("\\00*[D$3]$", "\\00*[D$3]\\Z", "^([a-z]*)([0-9]*)([a-z]*)$")
    val inputs = Seq("abcd", "abc012abc", "999abb", "\\00D", "D", "D\n", "\\00D\n\r")
    assertCpuGpuMatchesRegexpReplace(patterns, inputs)
  }

  test("zero-length repetition near line anchor  - regexp_split") {
    val patterns = Set("\\00*[D$3]$", "\\00*[D$3]\\Z", "^([a-z]*)([0-9]*)([a-z]*)$")
    patterns.foreach(pattern => {
      assertUnsupported(pattern, RegexSplitMode,
      "End of line/string anchor is not supported in this context")
    })
  }

  test("cuDF unsupported choice cases") {
    val patterns = Seq("c*|d*", "c*|dog", "c|d?", "c|d{0,2}")
    patterns.foreach(pattern => {
      assertUnsupported(pattern, RegexReplaceMode,
        "cuDF does not support replace or split with zero-length repetition on one side of a"
        + " choice")
    })
  }

  test("newline before $ in replace mode") {
    val patterns = Seq("\r$", "\n$", "\r\n$", "\u0085$", "\u2028$", "\u2029$")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, RegexReplaceMode,
        "End of line/string anchor is not supported in this context")
    )
  }

  test("cuDF does not support possessive quantifier") {
    val patterns = Seq("a*+", "a|(a?|a*+)")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, RegexFindMode,
        "Possessive quantifier *+ not supported")
    )
  }

  test("cuDF does not support \\z") {
      assertUnsupported("foo\\z", RegexFindMode,
        "\\z is not supported on GPU")
  }

  test("cuDF does not support positive or negative lookahead") {
    val negPatterns = Seq("a(!b)", "a(!b)c?")
    negPatterns.foreach(pattern =>
      assertUnsupported(pattern, RegexFindMode,
        "Negative lookahead groups are not supported")
    )

    val posPatterns = Seq("a(=b)", "a(=b)c?")
    posPatterns.foreach(pattern =>
      assertUnsupported(pattern, RegexFindMode,
        "Positive lookahead groups are not supported")
    )
  }

  test("cuDF does not support empty sequence") {
    val patterns = Seq("", "a|", "()")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, RegexFindMode, "Empty sequence not supported")
    )
  }

  test("cuDF does not support quantifier syntax when not quantifying anything") {
    // note that we could choose to transpile and escape the '{' and '}' characters
    val patterns = Seq("{1,2}", "{1,}", "{1}")
    patterns.foreach(pattern => {
      assertUnsupported(pattern, RegexFindMode,
        "Token preceding '{' is not quantifiable near index 0")
        }
    )

    val e = intercept[PatternSyntaxException] {
      parse("{2,1}")
    }
    assert(e.getMessage.contains("Illegal"))
  }

  test("cuDF does not support single repetition both inside and outside of capture groups") {
    val patterns = Seq("(3?)+", "(3?)*", "((3?))+")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, RegexFindMode,
        "cuDF does not support repetition of group containing: 3?"))
    Seq("(3*)+").foreach(pattern =>
      assertUnsupported(pattern, RegexFindMode,
        "cuDF does not support repetition of group containing: 3*"))
  }

  test("repetition base validation recurses into choices") {
    Seq("(3?|a)+", "(a|3?)+").foreach { pattern =>
      assertUnsupported(pattern, RegexFindMode,
        "cuDF does not support repetition of group containing: 3?")
    }
    Seq(
      "(3|a)+" -> "(3|a)+",
      raw"(a|\d)+" -> "(a|[0-9])+").foreach { case (pattern, expected) =>
      assert(transpile(pattern, RegexFindMode) === expected)
    }
  }

  test("cuDF does not support OR at BOL / EOL") {
    val patterns = Seq("$|a", "^|a")
    patterns.foreach(pattern => {
      assertUnsupported(pattern, RegexFindMode,
        "Sequences that only contain '^' or '$' are not supported")
    })
  }

  test("cuDF does not support class intersection &&") {
    val patterns = Seq("[a&&b]", "[&&1]")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, RegexFindMode,
        "cuDF does not support class intersection operator &&"))

    assertCpuGpuMatchesRegexpReplace(Seq("a&&b", "[a&b&c]"), Seq("a&&b", "b&c"))
  }

  test("octal digits - find") {
    val patterns = Seq(raw"\07", raw"\077", raw"\0177", raw"\01772", raw"\0200",
      raw"\0376", raw"\0377", raw"\02002")
    assertCpuGpuMatchesRegexpFind(patterns, Seq("", "\u0007", "a\u0007b", "a\u007fb",
        "\u0007\u003f\u007f", "\u007f", "\u0080", "a\u00fe\u00ffb", "\u007f2"))
  }

  test("octal digit character classes") {
    val patterns = Seq(raw"[\02]", raw"[\012]", raw"[\0177]", raw"[a-\0377]", raw"[\01-\0777]")
    val inputs = Seq("", "\u0002", "a\u0012b\n\u0177c", "a[+\u00fe23z")
    assertCpuGpuMatchesRegexpFind(patterns, inputs)
  }

  test("hex digits - find") {
    val patterns = Seq(raw"\x07", raw"\x3f", raw"\x7F", raw"\x7f", raw"\x{7}", raw"\x{0007f}",
      raw"\x80", raw"\xff", raw"\x{0008f}", raw"\x{10FFFF}", raw"\x{00eeee}")
    assertCpuGpuMatchesRegexpFind(patterns, Seq("", "\u0007", "a\u0007b",
        "\u0007\u003f\u007f", "\u0080", "a\u00fe\u00ffb", "ab\ueeeecd"))
  }

  test("hex digit character classes") {
    val patterns = Seq(raw"[\x02]", raw"[\x2c]", raw"[\x7f]", raw"[\x80]", raw"[\x01-\xff]",
      raw"[a-\xff]", raw"[\x20-z]")
    val inputs = Seq("", "\u007f", "a\u007fb", "\u007f\u003f\u007f", "\u0080", "a\u00fe\u00ffb",
      "\u007f2", "abcd", "\u0000\u007f\u00ff\u0123\u0abc")
    assertCpuGpuMatchesRegexpFind(patterns, inputs)
  }

  test("compare CPU and GPU: character range with escaped characters") {
    val inputs = Seq("", "abc", "\r\n", "12\u001b3", "a[b\t\n \rc]d", "[\r+\n-\t[]")
    assertCpuGpuMatchesRegexpFind(Seq(raw"[\r\n\t]", raw"[\t-\r]", raw"[\n-\\]", raw"[\a-\e]"),
      inputs)
    assertCpuGpuMatchesRegexpReplace(Seq("[\t-\r]", "[\b-\t123\n]", "[\\\\u002d-z]"),
      inputs)
  }

  test("string anchors - find") {
    val patterns = Seq("\\Atest", "\\A+test", "\\A{1}test", "\\A{1,}test")
    assertCpuGpuMatchesRegexpFind(patterns, Seq("", "test", "atest", "testa",
      "\ntest", "test\n", "\ntest\n"))
  }

  test("string anchor on one side of choice") {
    val patterns = Seq("a$|b", "^a|b", "\\Aa|b", "a\\Z|\\Ab")
    assertCpuGpuMatchesRegexpFind(patterns, Seq("", "testb", "atest", "testa",
      "\ntesta", "btesta\n", "\ntestab\n", "testa\r", "btesta\r\n"))
  }

  test("string anchor on both sides of choice") {
    val patterns = Seq("[abcd]$|^abc", "a$|b$", "^a|a$", "^a|b$", "\\Aa|a\\Z", "\\Aa|b\\Z")
    assertCpuGpuMatchesRegexpFind(patterns, Seq("", "testb", "atest", "testa",
      "\ntesta", "btesta\n", "\ntestab\n", "testa\r", "btesta\r\n"))
  }

  test("string anchor on either side of choice - regexp_replace") {
    val patterns = Seq("a$|b", "^a|b", "\\Aa|b", "a\\Z|\\Ab", "[abcd]$|^abc", "a$|b$", "^a|a$",
        "^a|b$", "\\Aa|a\\Z", "\\Aa|b\\Z")
    assertCpuGpuMatchesRegexpReplace(patterns, Seq("", "testb", "atest", "testa",
      "\ntesta", "btesta\n", "\ntestab\n", "testa\r", "btesta\r\n"))
  }

  test("string anchor on either side of choice - fallback to CPU - regexp_split") {
    val patterns = Set("a$|b", "^a|b", "\\Aa|b", "a\\Z|\\Ab", "[abcd]$|^abc", "a$|b$", "^a|a$",
        "^a|b$", "\\Aa|a\\Z", "\\Aa|b\\Z")
    patterns.foreach { pattern =>
      assertUnsupported(pattern, RegexSplitMode,
        "cuDF does not support either side of a choice containing a line anchor in split mode")
    }
  }

  test("string anchor \\A will fall back to CPU in some repetitions") {
    val patterns = Seq(raw"(\A)*a", raw"(\A){0,}a", raw"(\A){0}a")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, RegexFindMode,
        "cuDF does not support repetition of group containing: \\A")
    )
  }

  test("string anchor \\Z fall back to CPU in groups") {
    val patterns = Seq(raw"(\Z)a", raw"a(\Z)")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, RegexFindMode,
        "Line and string anchors are not supported in capture groups")
    )
  }

  test("string anchor \\Z fall back to CPU in some repetitions") {
    val patterns = Seq(raw"a(\Z)+", raw"a(\Z)*", raw"a(\Z){2,}")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, RegexFindMode,
        "cuDF does not support repetition of group containing: \\Z")
    )
  }

  test("string anchor \\Z fall back to CPU - split") {
    for (mode <- Seq(RegexSplitMode)) {
      assertUnsupported("\\Z", mode,
        "Sequences that only contain '^' or '$' are not supported")
    }
  }

  test("line anchors - replace") {
    val patterns = Seq("^test", "test$", "^test$", "test\\Z")
    assertCpuGpuMatchesRegexpReplace(patterns, Seq("", "test", "atest", "testa",
      "\ntest", "test\n", "\ntest\n", "\ntest\r\ntest\n"))
  }

  test("string anchors - replace") {
    val patterns = Seq("\\Atest", "test\\Z")
    assertCpuGpuMatchesRegexpReplace(patterns, Seq("", "test", "atest", "testa",
      "\ntest", "test\n", "\ntest\n", "\ntest\r\ntest\n"))
  }

  test("line anchor sequence $\\n fall back to CPU") {
    assertUnsupported("a$\n", RegexFindMode,
      "End of line/string anchor is not supported in this context")
  }

  test("issue-14746: anchors inside character classes are literals") {
    val patterns = Seq(
      """[$\n]""",
      """[$^]""",
      """[$]\n""",
      """\n[$]""",
      """(?:[$])\n""",
      """[a^]$""",
      """(?:[a^])$""")
    val inputs = Seq("", "$", "^", "\n", "$\n", "\n$", "a^", "a^\n", "a\nb")

    assertCpuGpuMatchesRegexpFind(patterns, inputs)
    assertCpuGpuMatchesRegexpReplace(patterns, inputs)

    // Character-class components are alternatives, not a sequence with anchor context.
    val characterClass = RegexCharacterClass(
      negated = false, ListBuffer(RegexChar('$'), RegexChar('\n')))
    val (transpiledClass, _) = new CudfRegexTranspiler(RegexReplaceMode)
      .getTranspiledAST(characterClass, None, None)
    assert(transpiledClass === characterClass)

    // Real anchors outside a class must still detect newlines inside the adjacent class.
    Seq("""[\r\n]$""", """$[\r\n]""").foreach { pattern =>
      assertUnsupported(pattern, RegexReplaceMode,
        "End of line/string anchor is not supported in this context")
    }
  }

  test("line anchor $ - find") {
    val patterns = Seq("a$", "a$b", "\f$", "$\f","TEST$")
    val inputs = Seq("a", "a\n", "a\r", "a\r\n", "a\f", "\f", "\r", "\u0085", "\u2028",
      "\u2029", "\n", "\r\n", "\r\n\r", "\r\n\u0085", "\n\r",
      "\n\u0085", "\n\u2028", "\n\u2029", "2+|+??wD\n", "a\r\nb",
      "TEST\u0085\n", "TEST\u0085\r", "TEST\u2028\r","TEST\u2028\u2029", "TEST\u2028\r\n")
    assertCpuGpuMatchesRegexpFind(patterns, inputs)
    val unsupportedPatterns = Seq("[\r\n]?$", "$\r", "\r$",
      // "\u0085$", "\u2028$", "\u2029$", "\n$", "\r\n$", "[D$3]$")
      "\u0085$", "\u2028$", "\u2029$", "\n$", "\r\n$")
    for (pattern <- unsupportedPatterns) {
      assertUnsupported(pattern, RegexFindMode,
        "End of line/string anchor is not supported in this context")
    }
  }

  test("string anchor \\Z - find") {
    val patterns = Seq("a\\Z", "a\\Zb", "a\\Z+", "\f\\Z", "\\Z\f")
    val inputs = Seq("a", "a\n", "a\r", "a\r\n", "a\f", "\f", "\r", "\u0085", "\u2028",
        "\u2029", "\n", "\r\n", "\r\n\r", "\r\n\u0085", "\n\r",
        "\n\u0085", "\n\u2028", "\n\u2029", "2+|+??wD\n", "a\r\nb")
    assertCpuGpuMatchesRegexpFind(patterns, inputs)
    val unsupportedPatterns = Seq("[\r\n]?\\Z", "\\Z\r", "\r\\Z",
      "\u0085\\Z", "\u2028\\Z", "\u2029\\Z", "\n\\Z", "\r\n\\Z")
    for (pattern <- unsupportedPatterns) {
      assertUnsupported(pattern, RegexFindMode,
        "End of line/string anchor is not supported in this context")
    }
  }

  test("word boundaries around \\D, \\S, \\W, \\H, or \\V - fall back to CPU") {
    val patterns = Seq("\\D\\B", "\\W\\B", "\\D\\b", "\\W\\b", "\\S\\b", "\\S\\B", "\\H\\B",
        "\\H\\b", "\\V\\B", "\\V\\b")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, RegexFindMode,
          "Word boundaries around \\D, \\S,\\W, \\H, or \\V are not supported")
    )
  }

  test("word boundaries around negated character class - fall back to CPU") {
    val patterns = Seq("[^A-Z]\\B", "[^A-Z]\\b")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, RegexFindMode,
        "Word boundaries around negated character classes are not supported")
    )
  }

  test ("word boundaries will fall back to CPU - split") {
    val patterns = Seq("\\b", "\\B")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, RegexSplitMode, "Word boundaries are not supported in split mode")
    )
  }

  test("whitespace boundaries - replace") {
    assertCpuGpuMatchesRegexpReplace(
      Seq("\\s", "\\S"),
      Seq("\u001eTEST"))
  }

  test("match literal $ - find") {
    assertCpuGpuMatchesRegexpFind(
      Seq("\\$", "\\$[0-9]"),
      Seq("", "$", "$9.99"))
  }

  test("match literal $ - replace") {
    assertCpuGpuMatchesRegexpReplace(
      Seq("\\$", "\\$[0-9]"),
      Seq("", "$", "$9.99"))
  }

  test("dot does not match all line terminators") {
    // see https://github.com/NVIDIA/spark-rapids/issues/5415
    val pattern = Seq("1.")
    val inputs = Seq("123", "1\r2", "1\n2", "1\r\n2", "1\u00852", "1\u20282", "1\u20292")
    assertCpuGpuMatchesRegexpFind(pattern, inputs)
  }

  test("dot does not match line terminator combinations") {
    val pattern = Seq("a.")
    val inputs = Seq("abc", "a\n\rb", "a\n\u0085b", "a\u2029\u0085b", "a\u2082\rb")
    assertCpuGpuMatchesRegexpFind(pattern, inputs)

  }

  test("replace_replace - ?, *, +, and {0, n} repetitions") {
    val patterns = Seq("D?", "D*", "D+", "D{0,}", "D{0,1}", "D{0,5}", "[1a-zA-Z]{0,}",
        "[1a-zA-Z]{0,2}", "A+")
    val inputs = Seq("SS", "DD", "SDSDSDS", "DDDD", "DDDDDD", "ABCDEFG")
    assertCpuGpuMatchesRegexpReplace(patterns, inputs)
  }

  test("dot matches CR on GPU but not on CPU") {
    // see https://github.com/rapidsai/cudf/issues/9619
    val pattern = "1."
    assertCpuGpuMatchesRegexpFind(Seq(pattern), Seq("1\r2", "1\n2", "1\r\n2"))
  }

  test("character class with ranges") {
    val patterns = Seq("[a-b]", "[a-zA-Z]")
    patterns.foreach(parse)
  }

  test("character class mixed") {
    val patterns = Seq("[a-b]", "[a+b]", "ab[cFef-g][^cat]")
    patterns.foreach(parse)
  }

  test("transpile character class unescaped range symbol") {
    val patterns = Seq("a[-b]", "a[+-]", "a[-+]", "a[-]", "a[^-]")
    val expected = Seq(raw"a[\-b]", raw"a[+\-]", raw"a[\-+]", raw"a[\-]", "a(?:[\r]|[^\\-])")
    val transpiler = new CudfRegexTranspiler(RegexFindMode)
    val transpiled = patterns.map(transpiler.transpile(_, None, None)._1)
    assert(transpiled === expected)
  }

  test("transpile complex regex 2") {
    val TIMESTAMP_TRUNCATE_REGEX = "^([0-9]{4}-[0-9]{2}-[0-9]{2} " +
      "[0-9]{2}:[0-9]{2}:[0-9]{2})" +
      "(.[1-9]*(?:0)?[1-9]+)?(.0*[1-9]+)?(?:.0*)?.\\Z"

    // input and output should be identical except for `.` being replaced
    // with `[^\n\r\u0085\u2028\u2029]` and `\z` being replaced with `$`
    doTranspileTest(TIMESTAMP_TRUNCATE_REGEX,
      TIMESTAMP_TRUNCATE_REGEX
        .replaceAll("\\.", "[^\n\r\u0085\u2028\u2029]")
        .replaceAll("\\\\Z", "\\$"))
  }

  test("transpile \\A repetitions") {
    doTranspileTest("a\\A+", "a\\A")
    doTranspileTest("a\\A{1,}", "a\\A")
    doTranspileTest("a\\A{2}", "a\\A")
  }

  test("transpile \\z") {
    assertUnsupported("abc\\z", RegexFindMode, "")
  }

  test("transpile \\Z") {
    val expected = "a$"
    doTranspileTest("a$", expected)
    doTranspileTest("a\\Z", expected)
    doTranspileTest("a\\Z+", expected)
    doTranspileTest("a\\Z{1}", expected)
    doTranspileTest("a\\Z{1,}", expected)
  }

  test("transpile predefined character classes Lower") {
    doTranspileTest("\\p{Lower}", "[a-z]")
  }

  test("transpile predefined character classes Alpha") {
    doTranspileTest("\\p{Alpha}", "[a-zA-Z]")
  }

  test("transpile predefined character classes Alnum") {
    doTranspileTest("\\p{Alnum}", "[a-zA-Z0-9]")
  }

  test("transpile predefined character classes Punct") {
    doTranspileTest("\\p{Punct}", "[!\"#$%&'()*+,\\-./:;<=>?@^_`{|}~\\[\\]\\\\]")
  }

  test("transpile predefined character classes Print") {
    doTranspileTest("\\p{Print}", "[a-zA-Z0-9!\"#$%&'()*+,\\-./:;<=>?@^_`{|}~\\[\\]\\\\\u0020]")
  }

  test("transpile with group index to extract") {
    doTranspileTest("(a)(b)", "(a)(?:b)", 1)
    doTranspileTest("(a)(b)", "(?:a)(b)", 2)
    doTranspileTest("(a)(b)(c)(d)", "(?:a)(?:b)(?:c)(?:d)", 0)
    doTranspileTest("(a)(?:b)(c)", "(a)(?:b)(?:c)", 1)
    doTranspileTest("(a)(b)(c)(d)", "(?:a)(?:b)(?:c)(?:d)", 5)
    doTranspileTest("(a(b))(c)(d)", "(a(?:b))(?:c)(?:d)", 1)
    doTranspileTest("(a(b))(c)(d)", "(?:a(b))(?:c)(?:d)", 2)
    doTranspileTest("(ab)+(c)(d)", "(ab)+(?:c)(?:d)", 1)
    doTranspileTest("(ab)+(c)(d)", "(?:ab)+(c)(?:d)", 2)
    doTranspileTest("([a-z0-9]((([abcd](\\d?)))))", "(?:[a-z0-9](?:((?:[abcd](?:[0-9]?)))))", 3)
    doTranspileTest("(a)|(b)", "(?:a)|(b)", 2)
    doTranspileTest("(?:(a)(b))", "(?:(?:a)(b))", 2)
    doTranspileTest("((a)|(b))", "(?:(?:a)|(b))", 3)
    doTranspileTest("(a)(b)|(c)(d)", "(?:a)(?:b)|(c)(?:d)", 3)
    doTranspileTest("ab", "ab", 1)
  }

  test("compare CPU and GPU: character range including unescaped + and -") {
    val patterns = Seq("a[-]+", "a[a-b-]+", "a[-a-b]", "a[-+]", "a[+-]")
    val inputs = Seq("a+", "a-", "a", "a-+", "a[a-b-]")
    assertCpuGpuMatchesRegexpFind(patterns, inputs)
  }

  test("compare CPU and GPU: character range including escaped + and - and d") {
    val patterns = Seq(raw"a[\-\+]", raw"a[\+\-]", raw"a[a-b\-]", raw"a[\d]", raw"a[\d\+]")
    val inputs = Seq("a+", "a-", "a", "a-+", "a[a-b-]", "a0", "a0+")
    assertCpuGpuMatchesRegexpFind(patterns, inputs)
  }

  test("compare CPU and GPU: octal") {
    val patterns = Seq("\\\\141")
    val inputs = Seq("a", "b")
    assertCpuGpuMatchesRegexpFind(patterns, inputs)
  }

  private val REGEXP_LIMITED_CHARS_COMMON = "|()[]{},-./;:!^$#%&*+?<=>@\"'~`_" +
    "abc0123x\\ \t\r\n\f\u000b\u0000BsdwSDWZ"

  private val REGEXP_LIMITED_CHARS_FIND = REGEXP_LIMITED_CHARS_COMMON

  private val REGEXP_LIMITED_CHARS_REPLACE = REGEXP_LIMITED_CHARS_COMMON

  test("compare CPU and GPU: find digits") {
    val patterns = Seq("\\d", "\\d+", "\\d*", "\\d?")
    val inputs = Seq("a", "1", "12", "a12z", "1az2")
    assertCpuGpuMatchesRegexpFind(patterns, inputs)
  }

  test("compare CPU and GPU: replace digits") {
    // note that we do not test with quantifiers `?` or `*` due
    // to https://github.com/NVIDIA/spark-rapids/issues/4468
    val patterns = Seq("\\d", "\\d+")
    val inputs = Seq("a", "1", "12", "a12z", "1az2")
    assertCpuGpuMatchesRegexpReplace(patterns, inputs)
  }

  test("compare CPU and GPU: regexp find fuzz test with limited chars") {
    // testing with this limited set of characters finds issues much
    // faster than using the full ASCII set
    // CR and LF has been excluded due to known issues
    doFuzzTest(Some(REGEXP_LIMITED_CHARS_FIND), RegexFindMode)
  }

  test("compare CPU and GPU: regexp replace simple regular expressions") {
    val inputs = Seq("a", "b", "c")
    val patterns = Seq("a|b")
    assertCpuGpuMatchesRegexpReplace(patterns, inputs)
  }

  test("compare CPU and GPU: regexp replace line anchor supported use cases") {
    val inputs = Seq("a", "b", "c", "cat", "", "^", "$", "^a", "t$")
    val patterns = Seq("^a", "^a", "(^a|^t)", "^[ac]", "^^^a", "[\\^^]", "a$", "a$$", "\\$$")
    assertCpuGpuMatchesRegexpReplace(patterns, inputs)
  }

  test("line anchor find - unicode line separators 0085") {
    assume(false, "https://github.com/NVIDIA/spark-rapids/issues/7585")
    val inputs = Seq("aTEST\u0085", "aTEST\u0085\n", "aTEST\n\u0085")
    assertCpuGpuMatchesRegexpFind(Seq("TEST$", "^a|T$"), inputs)
  }

  test("line anchor find - unicode line separators 2028") {
    assume(false, "https://github.com/NVIDIA/spark-rapids/issues/7585")
    val inputs = Seq("aTEST\u2028", "aTEST\u2028\n", "aTEST\n\u2028")
    assertCpuGpuMatchesRegexpFind(Seq("TEST$", "^a|T$"), inputs)
  }

  test("line anchor find - unicode line separators 2029") {
    assume(false, "https://github.com/NVIDIA/spark-rapids/issues/7585")
    val inputs = Seq("aTEST\u2029", "aTEST\u2029\n", "aTEST\n\u2029")
    assertCpuGpuMatchesRegexpFind(Seq("TEST$", "^a|T$"), inputs)
  }

  test("line anchor replace - unicode line separators 0085") {
    assume(false, "https://github.com/NVIDIA/spark-rapids/issues/7585")
    val inputs = Seq("aTEST\u0085", "aTEST\u0085\n", "aTEST\n\u0085")
    assertCpuGpuMatchesRegexpReplace(Seq("TEST$", "^a|T$"), inputs)
  }

  test("line anchor replace - unicode line separators 2028") {
    assume(false, "https://github.com/NVIDIA/spark-rapids/issues/7585")
    val inputs = Seq("aTEST\u2028", "aTEST\u2028\n", "aTEST\n\u2028")
    assertCpuGpuMatchesRegexpReplace(Seq("TEST$", "^a|T$"), inputs)
  }

  test("line anchor replace - unicode line separators 2029") {
    assume(false, "https://github.com/NVIDIA/spark-rapids/issues/7585")
    val inputs = Seq("aTEST\u2029", "aTEST\u2029\n", "aTEST\n\u2029")
    assertCpuGpuMatchesRegexpReplace(Seq("TEST$", "^a|T$"), inputs)
  }

  test("cuDF does not support some uses of line anchors in regexp_replace") {
    Seq("^", "$", "^*", "$*", "^+", "$+", "^|$", "^^|$$").foreach(
        pattern =>
      assertUnsupported(pattern, RegexReplaceMode,
        "Sequences that only contain '^' or '$' are not supported")
    )
    Seq("^$", "(^)($)", "(((^^^)))$").foreach(
      pattern =>
        assertUnsupported(pattern, RegexReplaceMode,
          "End of line/string anchor is not supported in this context")
    )
  }

  test("negated character class newline handling") {
    // tests behavior of com.nvidia.spark.rapids.CudfRegexTranspiler.negateCharacterClass

    def test(pattern: String, expected: String): Unit = {
      val t = new CudfRegexTranspiler(RegexFindMode)
      val (actual, _) = t.transpile(pattern, None, None)
      assert(toReadableString(expected) === toReadableString(actual))
    }

    test(raw"[^a]", raw"(?:[\r]|[^a])")
    test(raw"[^a\n]", raw"(?:[\r]|[^a\n])")
    test(raw"[^a\r]", raw"[^a\r]")
    test(raw"[^a\r\n]", raw"[^a\r\n]")
  }

  test("compare CPU and GPU: regexp replace negated character class") {
    val inputs = Seq(
      "a", "a\r", "a\n", "a\r\n", "a\n\r",
      "b", "b\r", "b\n", "b\r\n", "b\n\r",
      "a\nb", "b", "a\r\nb\n\rc\rd", "\r", "\r\n", "\n")
    val patterns = Seq(
      "[^a]", "[^a\r]", "[^a\n]", "[^a\r\n]",
      "[a]", "[a\r]", "[a\n]", "[a\r\n]",
      "[^b]", "[^b\r]", "[^b\n]", "[^b\r\n]",
      "[^z]", "[^\r]", "[^\n]", "[^\r]",
      "[^\r\n]", "[^b\r]", "[^bc\r\n]", "[^\\r\\n]", "[^\r\r]", "[^\r\n\r]", "[^\n\n\r\r]")
    assertCpuGpuMatchesRegexpReplace(patterns, inputs)
  }

  test("compare CPU and GPU: handle escaped brackets") {
    val inputs = Seq("[", "]", "[]", "[asdf]", "[deadbeef]", "[a123b456c]")
    val patterns = Seq("\\[", "\\]", "\\[]", "\\[\\]", "\\[([a-z]+)\\]", "\\[([a-z0-9]+)\\]")
    assertCpuGpuMatchesRegexpFind(patterns, inputs)
  }

  test("compare CPU and GPU: regexp replace fuzz test with limited chars") {
    // testing with this limited set of characters finds issues much
    // faster than using the full ASCII set
    // LF has been excluded due to known issues
    doFuzzTest(Some(REGEXP_LIMITED_CHARS_REPLACE), RegexReplaceMode)
  }

  test("compare CPU and GPU: regexp find fuzz test printable ASCII chars plus CR, LF, and TAB") {
    // CR and LF has been excluded due to known issues
    doFuzzTest(Some((0x20 to 0x7F).map(_.toChar) + "\r\n\t"), RegexFindMode)
  }

  test("compare CPU and GPU: fuzz test ASCII chars") {
    // LF has been excluded due to known issues
    val chars = (0x00 to 0x7F)
      .map(_.toChar)
    doFuzzTest(Some(chars.mkString), RegexReplaceMode)
  }

  test("compare CPU and GPU: regexp find fuzz test all chars") {
    // this test cannot be enabled until we support CR and LF
    doFuzzTest(None, RegexFindMode)
  }

  private def doFuzzTest(validChars: Option[String], mode: RegexMode): Unit = {

    val r = new EnhancedRandom(new Random(seed = 0L),
      options = FuzzerOptions(validChars, maxStringLen = 12))

    val data = Range(0, 1000)
      .map(_ => r.nextString())

    // generate patterns that are valid on both CPU and GPU
    val patterns = HashSet[String]()
    while (patterns.size < 5000) {
      val pattern = r.nextString()
      if (!patterns.contains(pattern)) {
        if (Try(Pattern.compile(pattern)).isSuccess && Try(transpile(pattern, mode)).isSuccess) {
          patterns += pattern
        }
      }
    }

    if (mode == RegexReplaceMode) {
      assertCpuGpuMatchesRegexpReplace(patterns.toSeq, data)
    } else {
      assertCpuGpuMatchesRegexpFind(patterns.toSeq, data)
    }
  }

  test("AST fuzz test - regexp_find") {
    doAstFuzzTest(Some(REGEXP_LIMITED_CHARS_FIND), REGEXP_LIMITED_CHARS_FIND,
      RegexFindMode)
  }

  test("AST fuzz test - regexp_replace") {
    doAstFuzzTest(Some(REGEXP_LIMITED_CHARS_REPLACE), REGEXP_LIMITED_CHARS_REPLACE,
      RegexReplaceMode)
  }

  test("AST fuzz test - regexp_find - full unicode input") {
    assume(isUnicodeEnabled())
    doAstFuzzTest(None, REGEXP_LIMITED_CHARS_REPLACE,
      RegexFindMode)
  }

  test("AST fuzz test - regexp_replace - full unicode input") {
    assume(isUnicodeEnabled())
    doAstFuzzTest(None, REGEXP_LIMITED_CHARS_REPLACE,
      RegexReplaceMode)
  }

  def isUnicodeEnabled(): Boolean = {
    Charset.defaultCharset().name() == "UTF-8"
  }

  test("AST fuzz test - regexp_find - anchor focused") {
    doAstFuzzTest(validDataChars = Some("\r\nabc"),
      validPatternChars = "^$\\AZz\r\n()[]-", mode = RegexFindMode)
  }

  test("string split - optimized") {
    val patterns = Set("\\.", "\\$", "\\[", "\\(", "\\}", "\\+", "\\\\", ",", ";", "cd", "c\\|d",
        "\\%", "\\;", "\\/")
    val data = Seq("abc.def", "abc$def", "abc[def]", "abc(def)", "abc{def}", "abc+def", "abc\\def",
        "abc,def", "abc;def", "abcdef", "abc|def", "abc%def")
    for (limit <- Seq(Integer.MIN_VALUE, -2, -1)) {
      assertTranspileToSplittableString(patterns)
      doStringSplitTest(patterns, data, limit)
    }
  }

  test("string split - not optimized") {
    val patterns = Set(".\\$", "\\[.", "\\(.", ".\\}", "\\+.", "c.\\|d")
    val data = Seq("abc.$def", "abc$def", "abc[def]", "abc(def)", "abc{def}", "abc+def", "abc\\def",
        "abc#|def")
    for (limit <- Seq(Integer.MIN_VALUE, -2, -1)) {
      assertNoTranspileToSplittableString(patterns)
      doStringSplitTest(patterns, data, limit)
    }
  }

  test("issue-14746: string split treats anchors in character classes as literals") {
    val patterns = Set(
      """[$\n]""",
      """[$^]""",
      """[$]\n""",
      "^a$",
      """\na$""",
      """[\r\n]a$""")
    val data = Seq("", "$", "^", "a", "\na", "\r\na", "$\n", "\n$", "a\nb")
    doStringSplitTest(patterns, data, -1)

    Seq("""[\r\n]$""", """$[\r\n]""").foreach { pattern =>
      assertUnsupported(pattern, RegexSplitMode,
        "End of line/string anchor is not supported in this context")
    }

    val optionalPrefixError = intercept[RegexUnsupportedException] {
      transpile("a?bc$", RegexSplitMode)
    }
    assert(optionalPrefixError.getMessage.endsWith("near index 4"),
      s"Unexpected split anchor position: ${optionalPrefixError.getMessage}")
  }

  test("issue-14748: word boundaries are not literal split delimiters") {
    assertNoTranspileToSplittableString(Set(raw"\b", raw"\B"))
  }

  test("regexp_split - character class repetition - ? and *") {
    val patterns = Set(raw"[a-z][0-9]?", raw"[a-z][0-9]*")
    val data = Seq("a", "aa", "a1a1", "a1b2", "a1b")
    for (limit <- Seq(Integer.MIN_VALUE, -2, -1)) {
      doStringSplitTest(patterns, data, limit)
    }
  }

  test("regexp_split - character class repetition - ? and * with reluctant quantifier") {
    val patterns = Set(raw"[a-z][0-9]??", raw"[a-z][0-9]*?")
    val data = Seq("a", "aa", "a1a1", "a1b2", "a1b")
    for (limit <- Seq(Integer.MIN_VALUE, -2, -1)) {
      doStringSplitTest(patterns, data, limit)
    }
  }

  test("regexp_split - repetition with {0,n}, or {0,}") {
    // see https://github.com/NVIDIA/spark-rapids/issues/6958
    val patterns = Set("ba{0,}", raw"a\02{0,}", "ba{0,2}", raw"b\02{0,10}")
    val data = Seq("abaa", "baba", "ba\u0002b", "ab\u0002b\u0002a")
    for (limit <- Seq(Integer.MIN_VALUE, -2, -1)) {
      doStringSplitTest(patterns, data, limit)
    }
  }

  test("regexp_split - character class repetition - ? and * - fall back to CPU") {
    // see https://github.com/NVIDIA/spark-rapids/issues/6958
    val patterns = Seq(raw"[1a-zA-Z]?", raw"[1a-zA-Z]*")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, RegexSplitMode,
        "regexp_split on GPU does not support empty match repetition consistently with Spark"
      )
    )
  }

  test("regexp_split - character class repetition with choice - ? and * - fall back to CPU") {
    val patterns = Seq(raw"c*|d*", raw"c*|dog", raw"a{0,2}|b", raw"b?|a", raw"[cat]{0,3}|dog")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, RegexSplitMode,
        "regexp_split on GPU does not support empty match repetition consistently with Spark"
      )
    )
  }

  test("regexp_split - fall back to CPU for {0,n}, or {0,}") {
    // see https://github.com/NVIDIA/spark-rapids/issues/6958
    val patterns = Seq("a{0,}", raw"\02{0,}", "a{0,2}", raw"\02{0,10}")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, RegexSplitMode,
        "regexp_split on GPU does not support empty match repetition consistently with Spark"
      )
    )
  }

  test("string split - limit < 0") {
    val patterns = Set("[^A-Z]+", "[0-9]+", ":", "o", "[:o]")
    val data = Seq("abc", "123", "1\n2\n3\n", "boo:and:foo")
    for (limit <- Seq(Integer.MIN_VALUE, -2, -1)) {
      doStringSplitTest(patterns, data, limit)
    }
  }

  test("string split - limit > 1") {
    val patterns = Set("[^A-Z]+", "[0-9]+", ":", "o", "[:o]")
    val data = Seq("abc", "123", "1\n2\n3\n", "boo:and:foo")
    for (limit <- Seq(2, 5, Integer.MAX_VALUE)) {
      doStringSplitTest(patterns, data, limit)
    }
  }

  test("string split fuzz") {
    val (data, patterns) = generateDataAndPatterns(Some(REGEXP_LIMITED_CHARS_REPLACE),
      REGEXP_LIMITED_CHARS_REPLACE, RegexSplitMode)
    for (limit <- Seq(-2, -1, 2, 5)) {
      doStringSplitTest(patterns, data, limit)
    }
  }

  test("string split fuzz - anchor focused") {
    val (data, patterns) = generateDataAndPatterns(validDataChars = Some("\r\nabc"),
      validPatternChars = "^$\\AZz\r\n()", RegexSplitMode)
    doStringSplitTest(patterns, data, -1)
  }

  def assertTranspileToSplittableString(patterns: Set[String]): Unit = {
    for (pattern <- patterns) {
      val transpiler = new CudfRegexTranspiler(RegexSplitMode)
      transpiler.transpileToSplittableString(pattern) match {
        case None =>
          fail(s"string_split pattern=${toReadableString(pattern)} " +
            "does not produce a simplified string to split on"
          )
        case _ =>
      }
    }
  }

  def assertNoTranspileToSplittableString(patterns: Set[String]): Unit = {
    for (pattern <- patterns) {
      val transpiler = new CudfRegexTranspiler(RegexSplitMode)
      transpiler.transpileToSplittableString(pattern) match {
        case Some(_) =>
          fail(s"string_split pattern=${toReadableString(pattern)} " +
            "is trying to produce a simplified string to split on when " +
            "it can't"
          )
        case _ =>
      }
    }
  }

  def doStringSplitTest(patterns: Set[String], data: Seq[String], limit: Int): Unit = {
    for (pattern <- patterns) {
      val cpu = cpuSplit(pattern, data, limit)
      val transpiler = new CudfRegexTranspiler(RegexSplitMode)
      val (isRegex, cudfPattern) = if (RegexParser.isRegExpString(pattern)) {
        transpiler.transpileToSplittableString(pattern) match {
          case Some(simplified) => (false, simplified)
          case _ => (true, transpiler.transpile(pattern, None, None)._1)
        }
      } else {
        (false, pattern)
      }
      val gpu = gpuSplit(cudfPattern, data, limit, isRegex)
      assert(cpu.length == gpu.length)
      for (i <- cpu.indices) {
        val cpuArray = cpu(i)
        val gpuArray = gpu(i)
        if (!cpuArray.sameElements(gpuArray)) {
          fail(s"string_split java pattern=${toReadableString(pattern)} " +
            s"cudfPattern=${toReadableString(cudfPattern)} " +
            s"isRegex=$isRegex " +
            s"data=${toReadableString(data(i))} limit=$limit " +
            s"\nCPU [${cpuArray.length}]: ${toReadableString(cpuArray.mkString(", "))} " +
            s"\nGPU [${gpuArray.length}]: ${toReadableString(gpuArray.mkString(", "))}")
        }
      }
    }
  }

  private def doAstFuzzTest(validDataChars: Option[String], validPatternChars: String,
      mode: RegexMode): Unit = {
    val (data, patterns) = generateDataAndPatterns(validDataChars, validPatternChars, mode)
    if (mode == RegexReplaceMode) {
      assertCpuGpuMatchesRegexpReplace(patterns.toSeq, data)
    } else {
      assertCpuGpuMatchesRegexpFind(patterns.toSeq, data)
    }
  }

  private def generateDataAndPatterns(
      validDataChars: Option[String],
      validPatternChars: String,
      mode: RegexMode): (Seq[String], Set[String]) = {

    val dataGen = new EnhancedRandom(new Random(seed = 0L),
      FuzzerOptions(validDataChars, maxStringLen = 12))

    val data = Range(0, 1000)
      .map(_ => dataGen.nextString())

    val skipUnicodeIssues = validDataChars match {
      case None => true
      case _ => false
    }

    // generate patterns that are valid on both CPU and GPU
    val fuzzer = new FuzzRegExp(validPatternChars, skipUnicodeIssues = skipUnicodeIssues)
    val patterns = HashSet[String]()
    while (patterns.size < 5000) {
      val pattern = fuzzer.generate(0).toRegexString
      if (!patterns.contains(pattern)) {
        if (Try(Pattern.compile(pattern)).isSuccess && Try(transpile(pattern, mode)).isSuccess) {
          patterns += pattern
        }
      }
    }
    (data, patterns.toSet)
  }

  private def assertCpuGpuMatchesRegexpFind(javaPatterns: Seq[String], input: Seq[String]): Unit = {
    for ((javaPattern, patternIndex) <- javaPatterns.zipWithIndex) {
      val cpu = cpuContains(javaPattern, input)
      val (cudfPattern, _) =
          new CudfRegexTranspiler(RegexFindMode).transpile(javaPattern, None, None)
      val gpu = try {
        gpuContains(cudfPattern, input)
      } catch {
        case e: CudfException =>
          fail(s"cuDF failed to compile pattern: ${toReadableString(cudfPattern)}", e)
      }
      for (i <- input.indices) {
        if (cpu(i) != gpu(i)) {
          fail(s"javaPattern[$patternIndex]=${toReadableString(javaPattern)}, " +
            s"cudfPattern=${toReadableString(cudfPattern)}, " +
            s"input='${toReadableString(input(i))}', " +
            s"cpu=${cpu(i)}, gpu=${gpu(i)}")
        }
      }
    }
  }

  private def assertCpuGpuMatchesRegexpReplace(
      javaPatterns: Seq[String],
      input: Seq[String]): Unit = {
    for ((javaPattern, patternIndex) <- javaPatterns.zipWithIndex) {
      val cpu = cpuReplace(javaPattern, input)
      val (cudfPattern, replaceString) =
          (new CudfRegexTranspiler(RegexReplaceMode)).transpile(javaPattern,
              None, Some(REPLACE_STRING))
      val gpu = try {
        gpuReplace(cudfPattern, replaceString.get, input)
      } catch {
        case e: CudfException =>
          fail(s"cuDF failed to compile pattern: ${toReadableString(cudfPattern)}, " +
              s"original: ${toReadableString(javaPattern)}, " +
              s"replacement: ${toReadableString(replaceString.get)}", e)
      }
      for (i <- input.indices) {
        if (cpu(i) != gpu(i)) {
          fail(s"javaPattern[$patternIndex]=${toReadableString(javaPattern)}, " +
            s"cudfPattern=${toReadableString(cudfPattern)}, " +
            s"input='${toReadableString(input(i))}', " +
            s"cpu=${toReadableString(cpu(i))}, " +
            s"gpu=${toReadableString(gpu(i))}")
        }
      }
    }
  }

  /** cuDF containsRe helper */
  private def gpuContains(cudfPattern: String, input: Seq[String]): Array[Boolean] = {
    val result = new Array[Boolean](input.length)
    withResource(ColumnVector.fromStrings(input: _*)) { cv =>
      val prog = new RegexProgram(cudfPattern,
        EnumSet.of(RegexFlag.EXT_NEWLINE) ,CaptureGroups.NON_CAPTURE)
      withResource(cv.containsRe(prog)) { c =>
        withResource(c.copyToHost()) { hv =>
          result.indices.foreach(i => result(i) = hv.getBoolean(i))
        }
      }
    }
    result
  }

  private val REPLACE_STRING = "\\_\\RE\\\\P\\L\\A\\C\\E\\_"

  /** cuDF replaceRe helper */
  private def gpuReplace(cudfPattern: String, replaceString: String,
      input: Seq[String]): Array[String] = {
    val result = new Array[String](input.length)
    val replace = GpuRegExpUtils.unescapeReplaceString(replaceString)
    // Pass -1 to opt out of Java-spec greedy-with-backoff; the REPLACE_STRING used by this
    // helper contains no `$N` backrefs, so the legacy behavior is sufficient here.
    val (hasBackrefs, converted) = GpuRegExpUtils.backrefConversion(replace, -1)
    withResource(ColumnVector.fromStrings(input: _*)) { cv =>
      val c = if (hasBackrefs) {
        cv.stringReplaceWithBackrefs(new RegexProgram(cudfPattern,
          EnumSet.of(RegexFlag.EXT_NEWLINE)), converted)
      } else {
        withResource(GpuScalar.from(converted, DataTypes.StringType)) { replace =>
          val prog = new RegexProgram(cudfPattern,
            EnumSet.of(RegexFlag.EXT_NEWLINE), CaptureGroups.NON_CAPTURE)
          cv.replaceRegex(prog, replace)
        }
      }
      withResource(c) { c =>
        withResource(c.copyToHost()) { hv =>
          result.indices.foreach(i => result(i) = new String(hv.getUTF8(i)))
        }
      }
    }
    result
  }

  private def cpuContains(pattern: String, input: Seq[String]): Array[Boolean] = {
    val p = Pattern.compile(pattern)
    input.map(s => p.matcher(s).find(0)).toArray
  }

  private def cpuReplace(pattern: String, input: Seq[String]): Array[String] = {
    val p = Pattern.compile(pattern)
    input.map(s => p.matcher(s).replaceAll(REPLACE_STRING)).toArray
  }

  private def cpuSplit(pattern: String, input: Seq[String], limit: Int): Seq[Array[String]] = {
    input.map(s => s.split(pattern, limit))
  }

  private def gpuSplit(
      pattern: String,
      input: Seq[String],
      limit: Int,
      isRegex: Boolean): Seq[Array[String]] = {
    withResource(ColumnVector.fromStrings(input: _*)) { cv =>
      val x = if (isRegex) {
        cv.stringSplitRecord(new RegexProgram(pattern,
          EnumSet.of(RegexFlag.EXT_NEWLINE), CaptureGroups.NON_CAPTURE), limit)
      } else {
        cv.stringSplitRecord(pattern, limit)
      }
      withResource(x) { x =>
        withResource(x.copyToHost()) { hcv =>
          (0 until hcv.getRowCount.toInt).map(i => {
            val list = hcv.getList(i)
            list.toArray(new Array[String](list.size()))
          })
        }
      }
    }
  }

  private def doTranspileTest(pattern: String, expected: String): Unit = {
    val transpiled: String = transpile(pattern, RegexFindMode)
    assert(toReadableString(transpiled) === toReadableString(expected))
  }

  private def doTranspileTest(pattern: String, expected: String, groupIdx: Int): Unit = {
    val transpiled: String = transpile(pattern, groupIdx)
    assert(toReadableString(transpiled) === toReadableString(expected))
  }

  private def transpile(pattern: String, mode: RegexMode): String = {
    val replace = mode match {
      case RegexReplaceMode => Some(REPLACE_STRING)
      case _ => None
    }
    val (cudfPattern, _) = new CudfRegexTranspiler(mode).transpile(pattern, None, replace)
    cudfPattern
  }

  private def transpile(pattern: String, groupIndex: Int): String = {
    val (cudfPattern, _) = new CudfRegexTranspiler(RegexFindMode).transpile(
        pattern, Some(groupIndex), None)
    cudfPattern
  }


  private def assertUnsupported(pattern: String, mode: RegexMode, message: String): Unit = {
    val e = withClue(s"Pattern: '${toReadableString(pattern)}':") {
      intercept[RegexUnsupportedException] {
        transpile(pattern, mode)
      }
    }
    val msg = e.getMessage
    if (!msg.startsWith(message)) {
      fail(s"Pattern '${toReadableString(pattern)}': Error was [${e.getMessage}]"
        + s" but expected [$message]'")
    }
    if(!msg.contains("near index")) {
      fail(s"Pattern '${toReadableString(pattern)}': Error was [${e.getMessage}]"
        + " but does not specify index")
    }
  }

  private def parse(pattern: String): RegexAST = new RegexParser(pattern).parse()

}

/**
 * Generates random regular expression patterns by building an AST and then
 * converting to a string. This results in better coverage than randomly
 * generating strings directly.
 *
 * See https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html for
 * Java regular expression syntax.
 */
class FuzzRegExp(suggestedChars: String, skipKnownIssues: Boolean = true,
    skipUnicodeIssues: Boolean = false) {
  private val maxDepth = 5
  private val rr = new Random(0)

  val chars = if (skipKnownIssues) {
    // skip '\\' and '-' due to https://github.com/NVIDIA/spark-rapids/issues/4505
    suggestedChars.filterNot(ch => "\\-".contains(ch))
  } else {
    suggestedChars
  }

  def generate(depth: Int): RegexAST = {
    if (depth == maxDepth) {
      // when we reach maximum depth we generate a non-nested type
      nonNestedTerm
    } else {
      val generators: Seq[() => RegexAST] = Seq(
        () => lineTerminator,
        () => escapedChar,
        () => char,
        () => hexDigit,
        () => octalDigit,
        () => characterClass,
        () => predefinedCharacterClass,
        () => group(depth),
        () => boundaryMatch,
        () => sequence(depth),
        () => repetition(depth),
        () => choice(depth))
      generators(rr.nextInt(generators.length))()
    }
  }

  private def nonNestedTerm: RegexAST = {
    val generators: Seq[() => RegexAST] = Seq(
      () => lineTerminator,
      () => escapedChar,
      () => char,
      () => hexDigit,
      () => octalDigit,
      () => charRange,
      () => boundaryMatch,
      () => predefinedCharacterClass
    )
    generators(rr.nextInt(generators.length))()
  }

  private def characterClassComponent = {
    val generators = Seq[() => RegexCharacterClassComponent](
        () => char,
        () => charRange,
        () => hexDigit,
        () => octalDigit,
        () => escapedChar)
    generators(rr.nextInt(generators.length))()
  }

  private def charRange: RegexCharacterClassComponent = {
    val generators = Seq[() => RegexCharacterClassComponent](
      () => RegexCharacterRange(RegexChar('a'), RegexChar('z')),
      () => RegexCharacterRange(RegexChar('A'), RegexChar('Z')),
      () => RegexCharacterRange(RegexChar('z'), RegexChar('a')),
      () => RegexCharacterRange(RegexChar('Z'), RegexChar('A')),
      () => RegexCharacterRange(RegexChar('0'), RegexChar('9')),
      () => RegexCharacterRange(RegexChar('9'), RegexChar('0')),
      () => RegexCharacterRange(char, char)
    )
    generators(rr.nextInt(generators.length))()
  }

  private def sequence(depth: Int) = {
    val b = new ListBuffer[RegexAST]()
    b.appendAll(Range(0, 3).map(_ => generate(depth + 1)))
    RegexSequence(b)
  }

  private def characterClass = {
    val characters = new ListBuffer[RegexCharacterClassComponent]()
    characters.appendAll(Range(0, 3).map(_ => characterClassComponent))
    RegexCharacterClass(negated = rr.nextBoolean(), characters = characters)
  }

  private def char: RegexChar = {
    RegexChar(chars(rr.nextInt(chars.length)))
  }

  /** Any escaped character */
  private def escapedChar: RegexEscaped = {
    var ch = '\u0000'
    do {
      ch = chars(rr.nextInt(chars.length))
      // see https://github.com/NVIDIA/spark-rapids/issues/5882 for \B and \b issue
    } while (skipUnicodeIssues && "bB".contains(ch))
    RegexEscaped(ch)
  }

  private def lineTerminator: RegexAST = {
    val generators = Seq[() => RegexAST](
      () => RegexChar('\r'),
      () => RegexChar('\n'),
      () => RegexSequence(ListBuffer(RegexChar('\r'), RegexChar('\n'))),
      () => RegexChar('\u0085'),
      () => RegexChar('\u2028'),
      () => RegexChar('\u2029')
    )
    generators(rr.nextInt(generators.length))()
  }

  private def boundaryMatch: RegexAST = {
    val baseGenerators = Seq[() => RegexAST](
      () => RegexChar('^'),
      () => RegexChar('$'),
      () => RegexEscaped('A'),
      () => RegexEscaped('G'),
      () => RegexEscaped('Z'),
      () => RegexEscaped('z')
    )
    val generators = if (skipUnicodeIssues) {
      baseGenerators
    } else {
      baseGenerators ++ Seq[() => RegexAST](
        // see https://github.com/NVIDIA/spark-rapids/issues/5882 for \B and \b issue
        () => RegexEscaped('b'),
        () => RegexEscaped('B'))
    }
    generators(rr.nextInt(generators.length))()
  }

  private def predefinedCharacterClass: RegexAST = {
    val generators = Seq[() => RegexAST](
      () => RegexChar('.'),
      () => RegexEscaped('d'),
      () => RegexEscaped('D'),
      () => RegexEscaped('s'),
      () => RegexEscaped('S'),
      () => RegexEscaped('w'),
      () => RegexEscaped('W')
    )
    generators(rr.nextInt(generators.length))()
  }

  private def hexDigit: RegexHexDigit = {
    // \\xhh      The character with hexadecimal value 0xhh
    // \\uhhhh    The character with hexadecimal value 0xhhhh
    // \\x{h...h} The character with hexadecimal value 0xh...h
    //            (Character.MIN_CODE_POINT  <= 0xh...h <=  Character.MAX_CODE_POINT)
    val generators: Seq[() => String] = Seq(
      () => rr.nextInt(0xFF).toHexString,
      () => rr.nextInt(0xFFFF).toHexString,
      () => Character.MIN_CODE_POINT.toHexString,
      () => Character.MAX_CODE_POINT.toHexString
    )
    RegexHexDigit(generators(rr.nextInt(generators.length))())
  }

  private def octalDigit: RegexOctalChar = {
    // \\0n   The character with octal value 0n (0 <= n <= 7)
    // \\0nn  The character with octal value 0nn (0 <= n <= 7)
    // \\0mnn The character with octal value 0mnn (0 <= m <= 3, 0 <= n <= 7)
    val chars = "01234567"
    val generators: Seq[() => String] = Seq(
      () => Range(0,1).map(_ => chars(rr.nextInt(chars.length))).mkString,
      () => Range(0,2).map(_ => chars(rr.nextInt(chars.length))).mkString,
      () =>
        // this will generate some invalid octal numbers were the first digit > 3
        Range(0,3).map(_ => chars(rr.nextInt(chars.length))).mkString
    )
    RegexOctalChar("0" + generators(rr.nextInt(generators.length))())
  }

  private def choice(depth: Int) = {
    RegexChoice(generate(depth + 1), generate(depth + 1))
  }

  private def group(depth: Int) = {
    RegexGroup(capture = rr.nextBoolean(), generate(depth + 1), None)
  }

  private def repetition(depth: Int) = {
    val generators = Seq(
      () =>
        // greedy quantifier
        RegexRepetition(generate(depth + 1), quantifier),
      () =>
        // reluctant quantifier
        RegexRepetition(RegexRepetition(generate(depth + 1), quantifier),
          SimpleQuantifier('?')),
      () =>
        // possessive quantifier
        RegexRepetition(RegexRepetition(generate(depth + 1), quantifier),
          SimpleQuantifier('+'))
    )
    generators(rr.nextInt(generators.length))()
  }

  private def quantifier: RegexQuantifier = {
    val generators = Seq[() => RegexQuantifier](
      () => SimpleQuantifier('+'),
      () => SimpleQuantifier('*'),
      () => SimpleQuantifier('?'),
      () => QuantifierFixedLength(rr.nextInt(3)),
      () => QuantifierVariableLength(rr.nextInt(3), None),
      () => {
        // this intentionally generates some invalid quantifiers where the maxLength
        // is less than the minLength, such as "{2,1}" which should be handled as a
        // literal string match on "{2,1}" rather than as a valid quantifier.
        QuantifierVariableLength(rr.nextInt(3), Some(rr.nextInt(3)))
      }
    )
    generators(rr.nextInt(generators.length))()
  }
}
