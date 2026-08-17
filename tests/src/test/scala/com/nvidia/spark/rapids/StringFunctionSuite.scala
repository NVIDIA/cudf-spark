/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
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

import org.scalatest.Ignore
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, lower, upper}
import org.apache.spark.sql.rapids.GpuRegExpUtils
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims._

 /*
 * Different versions of Java support different versions of Unicode.
 *
 * java 8  :  unicode 6.2
 * java 9  :  unicode 8.0     (unsupported)
 * java 10 :  unicode 8.0     (unsupported)
 * java 11 :  unicode 10.0
 * java 12 :  unicode 11.0    (unsupported)
 * java 13 :  unicode 12.1
 * java 17 :  unicode 13.0    (unsupported)
 *
 */
object SupportedUnicodeVersion extends Enumeration {
  val UNICODE_6 = 0
  val UNICODE_10 = 1
  val UNICODE_12 = 2
  val UNICODE_13 = 3

  val NUM_UNICODE_VERSIONS = 4
  val UNICODE_UNSUPPORTED = 5

  val currentSupportedVersion = UNICODE_12
}

/*
 * Cudf only supports a single version of Unicode (currently, 12.1). However, people running
 * various versions of Java that support different unicode versions will see different results
 * from Spark cpu. The lists below represent characters which we know will come up 'incorrect'
 * during a test, depending on Java/unicode version. To work around this, we maintain lists of
 * known bad characters for earlier versions of Java/unicode so that the tests don't break.
 */
object CudfIncompatibleCodepoints {
  val uppercaseIncompatible = Array[List[Int]](
                                  // Java 8 / unicode 6.2
                                  List( 604,   609,   618,   620,   642,   647,   669,   670,
                                        1011,  4304,  4305,  4306,  4307,  4308,  4309,  4310,
                                        4311,  4312,  4313,  4314,  4315,  4316,  4317,  4318,
                                        4319,  4320,  4321,  4322,  4323,  4324,  4325,  4326,
                                        4327,  4328,  4329,  4330,  4331,  4332,  4333,  4334,
                                        4335,  4336,  4337,  4338,  4339,  4340,  4341,  4342,
                                        4343,  4344,  4345,  4346,  4349,  4350,  4351,  7566),

                                  // java 11, unicode 10
                                  List( 642,   4304,  4305,  4306,  4307,  4308,  4309,  4310,
                                        4311,  4312,  4313,  4314,  4315,  4316,  4317,  4318,
                                        4319,  4320,  4321,  4322,  4323,  4324,  4325,  4326,
                                        4327,  4328,  4329,  4330,  4331,  4332,  4333,  4334,
                                        4335,  4336,  4337,  4338,  4339,  4340,  4341,  4342,
                                        4343,  4344,  4345,  4346,  4349,  4350,  4351,  7566,
                                        42900),

                                  // java 13, unicode 12
                                  List(),

                                  // java 17, unicode 13
                                  List(411, 612, 42952, 42954, 42998))

  val lowercaseIncompatible = Array[List[Int]](
                                  // Java 8 / unicode 6.2
                                  List( 5024,  5025,  5026,  5027,  5028,  5029,  5030,  5031,
                                        5032,  5033,  5034,  5035,  5036,  5037,  5038,  5039,
                                        5040,  5041,  5042,  5043,  5044,  5045,  5046,  5047,
                                        5048,  5049,  5050,  5051,  5052,  5053,  5054,  5055,
                                        5056,  5057,  5058,  5059,  5060,  5061,  5062,  5063,
                                        5064,  5065,  5066,  5067,  5068,  5069,  5070,  5071,
                                        5072,  5073,  5074,  5075,  5076,  5077,  5078,  5079,
                                        5080,  5081,  5082,  5083,  5084,  5085,  5086,  5087,
                                        5088,  5089,  5090,  5091,  5092,  5093,  5094,  5095,
                                        5096,  5097,  5098,  5099,  5100,  5101,  5102,  5103,
                                        5104,  5105,  5106,  5107,  5108),

                                  // java 11, unicode 10
                                  List(),

                                  // java 13, unicode 12
                                  List(),

                                  // java 17, unicode 13
                                  List(42951, 42953, 42997))
}

/*
 * Different versions of Java support different versions of Unicode.  this class provides
 * the valid list of codepoints for whatever version of Java is being run.  It also
 * provides filtered lists of codepoints that are known to be broken in cudf.
 *
 * java 8  :  unicode 6.2
 * java 9  :  unicode 8.0     (unsupported)
 * java 10 :  unicode 8.0     (unsupported)
 * java 11 :  unicode 10.0
 * java 12 :  unicode 11.0    (unsupported)
 * java 13 :  unicode 12.1
 * java 17 :  unicode 13.0    (unsupported)
 *
 */
object TestCodepoints {
  val validCodepoints = (0 until 65536).filter(Character.isDefined).map(i => (i, i.toChar.toString))
  val validCodepointIndices = validCodepoints.map(tuple => tuple._1)

  // determine what 'supported' version of unicode we're using, if any
  def getActiveUnicodeVersion(): Int = {
    val vp = System.getProperties().getProperty("java.specification.version").split('.')

    // java <= 8
    if (vp(0).toInt == 1 && vp.length > 1) {
      if(vp(1).toInt == 8){
        return SupportedUnicodeVersion.UNICODE_6
      }
      return SupportedUnicodeVersion.UNICODE_UNSUPPORTED
    }

    // java 9+
    vp(0).toInt match {
      case 11 => SupportedUnicodeVersion.UNICODE_10
      case 13 => SupportedUnicodeVersion.UNICODE_12
      case 17 => SupportedUnicodeVersion.UNICODE_13
      case _ => SupportedUnicodeVersion.UNICODE_UNSUPPORTED
    }
  }
  // print out a warning if we're on an unsupported version
  if (getActiveUnicodeVersion() == SupportedUnicodeVersion.UNICODE_UNSUPPORTED) {
    printf("WARNING : Unsupported version of Java (%s). You may encounter unexpected " +
      "test failures\n", System.getProperties().getProperty("java.specification.version"))
  }

  // get the unicode index to use. if we are on an unknown/unsupported version, just
  // default to unicode 12
  def getUnicodeIncompatibleIndex(): Int = {
    val version = getActiveUnicodeVersion()
    if (version == SupportedUnicodeVersion.UNICODE_UNSUPPORTED) {
      SupportedUnicodeVersion.UNICODE_12
    } else {
      version
    }
  }

  // all unicode codepoints valid for this particular version of Java/Unicode.
  def validCodepointCharsDF(session: SparkSession): DataFrame = {
    import session.implicits._
    validCodepoints.toDF("indices", "strings")
  }

  // codepoint chars that we know should be working.  known issues in cudf are filtered out here
  def uppercaseCompatibleCharsDF(session: SparkSession): DataFrame = {
    import session.implicits._
    val version = getUnicodeIncompatibleIndex()
    val utf8Chars = (validCodepointIndices diff
      CudfIncompatibleCodepoints.uppercaseIncompatible(version)).map(i => i.toChar.toString)
    utf8Chars.toDF("strings")
  }

  // codepoint chars that we know should be working.  known issues in cudf are filtered out here
  def lowercaseCompatibleCharsDF(session: SparkSession): DataFrame = {
    import session.implicits._
    val version = getUnicodeIncompatibleIndex()
    val utf8Chars = (validCodepointIndices diff
      CudfIncompatibleCodepoints.lowercaseIncompatible(version)).map(i => i.toChar.toString)
    utf8Chars.toDF("strings")
  }
}

class StringOperatorsSuite extends SparkQueryCompareTestSuite {
  INCOMPAT_testSparkResultsAreEqual("Test compatible values upper case modifier",
    TestCodepoints.uppercaseCompatibleCharsDF) {
    frame => frame.select(upper(col("strings")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test compatible values lower case modifier",
    TestCodepoints.lowercaseCompatibleCharsDF) {
    frame => frame.select(lower(col("strings")))
  }
}

class RegExpUtilsSuite extends AnyFunSuite {
  test("countGroups ignores non-capturing groups") {
    val cases = Seq(
      "(?:(a))" -> 1,
      "(?:(a)(b))" -> 2,
      "(?:(a)|(b))" -> 2,
      "(x)(?:(a)|(b))(y)" -> 4)

    cases.foreach { case (pattern, expected) =>
      assert(GpuRegExpUtils.countGroups(pattern) == expected)
    }
  }

  test("get list of choices from regexp for multi-replace") {
    val regexChoices = Map(
      "aa|bb" -> Seq("aa", "bb"),
      "(aa)|(bb)" -> Seq("aa", "bb"),
      "aa|bb|cc" -> Seq("aa", "bb", "cc"),
      "(aa)|(bb)|(cc)" -> Seq("aa", "bb", "cc"),
      "aa|bb|cc|dd" -> Seq("aa", "bb", "cc", "dd"),
      "(aa|bb)|(cc|dd)" -> Seq("aa", "bb", "cc", "dd"),
      "aa|bb|cc|dd|ee" -> Seq("aa", "bb", "cc", "dd", "ee"),
      "aa|bb|cc|dd|ee|ff" -> Seq("aa", "bb", "cc", "dd", "ee", "ff"),
      "foo(cat)" -> Seq("foocat"),
      "(foo)(cat)" -> Seq("foocat"),
      "(foo)(cat)|(bar)(dog)" -> Seq("foocat", "bardog"),
      "a\n|b\t|c\r" -> Seq("a\n", "b\t", "c\r")
    )

    regexChoices.foreach { case (pattern, choices) =>
      val (ast, _) = (new CudfRegexTranspiler(RegexReplaceMode)).getTranspiledAST(pattern,
              None, Some(""))
      val result = GpuRegExpUtils.getChoicesFromRegex(ast)
      assert(result.isDefined && result.forall(_ == choices))
    }

    Seq("foo(cat|dog)", "(cat|dog)foo").foreach { pattern =>
      val (ast, _) = (new CudfRegexTranspiler(RegexReplaceMode)).getTranspiledAST(pattern,
        None, Some(""))
      assert(GpuRegExpUtils.getChoicesFromRegex(ast).isEmpty,
        s"mixed sequence must not use stringReplaceMulti: $pattern")
    }

    val emptySequence = RegexSequence(
      scala.collection.mutable.ListBuffer.empty[RegexAST])
    assert(GpuRegExpUtils.getChoicesFromRegex(emptySequence).isEmpty)

  }

  test("issue-14743: backrefConversion greedy-with-backoff per Java spec") {
    // (numCaptureGroups, replacement, expectedHasBackref, expectedConverted).
    // Java's `Matcher.appendReplacement` reads digits one at a time and stops as
    // soon as the running group index would exceed the capture-group count.
    // Note: literal "${...}" tokens are spelled with concatenation so scalastyle
    // doesn't flag them as missing string interpolation.
    val open = "$" + "{"
    val cases: Seq[(Int, String, Boolean, String)] = Seq(
      // 2 groups, "$12": stop after "1" (because 12 > 2); remaining "2" is literal.
      (2, "$12", true, open + "1}2"),
      // 20 groups, "$12": both digits consumed (12 <= 20).
      (20, "$12", true, open + "12}"),
      // 2 groups, "$123": stop after "1"; "23" trail as literals.
      (2, "$123", true, open + "1}23"),
      // 2 groups, "$2": one digit consumed.
      (2, "$2", true, open + "2}"),
      // 0 groups, "$1": legacy path -- emit ${1} so cuDF surfaces the error.
      (0, "$1", true, open + "1}"),
      // Java replacement strings treat `\digit` as the literal digit, not a backref.
      (2, "\\12", false, "\\12"),
      // No digits after `$` -- literal `$`.
      (2, "$a", false, "$a"),
      // `$0` is the whole-match backref and is always valid (cuDF supports group 0).
      (2, "$0", true, open + "0}"),
      // Leading zeroes participate in the Java greedy-with-backoff parse.
      (1, "$09", true, open + "0}9"),
      (0, "$01", true, open + "0}1"),
      (2, "$001", true, open + "1}"),
      // Numbers in the middle: "x$12y" with 2 groups -> "x${1}2y".
      (2, "x$12y", true, "x" + open + "1}2y"),
      // First digit alone would already exceed the count: fall back to the legacy
      // eagerly-greedy path so cuDF errors out as before (covered by
      // test_re_replace_backrefs_idx_out_of_bounds in regexp_test.py).
      (4, "[$5]", true, "[" + open + "5}]"),
      // Negative numCaptureGroups disables the greedy-with-backoff check (legacy
      // behavior preserved for callers that don't know the group count).
      (-1, "$12", true, open + "12}")
    )
    cases.foreach { case (numGroups, rep, expectedHas, expectedConv) =>
      val (hasBackref, converted) = GpuRegExpUtils.backrefConversion(rep, numGroups)
      assert(hasBackref == expectedHas,
        s"hasBackref mismatch for ($numGroups, $rep): got $hasBackref, want $expectedHas")
      assert(converted == expectedConv,
        s"converted mismatch for ($numGroups, $rep): got '$converted', want '$expectedConv'")
    }
  }

  test("line-anchor `$` no longer injects a synthetic capture group or generated backref " +
      "(#15006, cuDF #22763)") {
    // cuDF #22763 makes EXT_NEWLINE treat `\r\n` as a single line terminator for `$`, so the
    // transpiler emits a bare `$` instead of the old synthetic `(\r\n)?$` capture group. With no
    // synthetic group there is no internally-generated backref appended to the replacement, and a
    // user pattern ending in `$` no longer shifts backref numbering: user `$N` parses against the
    // real user group count (Java spec).
    val open = "$" + "{"
    val userPattern = "(T)(E)(S)(T)(T)(E)(S)(T)(T)(E)(S)(T)$"
    val userNumCaptureGroups =
      java.util.regex.Pattern.compile(userPattern).matcher("").groupCount()
    assert(userNumCaptureGroups == 12)

    // user replacement `$123$2` -> `$12` + literal `3` + `$2`, with no trailing generated group.
    val (_, repl1) = new CudfRegexTranspiler(RegexReplaceMode)
      .getTranspiledAST(userPattern, None, Some("$123$2"))
    val (has1, conv1) =
      GpuRegExpUtils.backrefConversion(repl1.get.toRegexString, userNumCaptureGroups)
    assert(has1)
    assert(conv1 == open + "12}3" + open + "2}")

    // user replacement `$13` with only 12 user groups MUST back off to `$1` + literal `3`.
    val (_, repl2) = new CudfRegexTranspiler(RegexReplaceMode)
      .getTranspiledAST(userPattern, None, Some("$13"))
    val (has2, conv2) =
      GpuRegExpUtils.backrefConversion(repl2.get.toRegexString, userNumCaptureGroups)
    assert(has2)
    assert(conv2 == open + "1}3",
      s"expected user `$$13` to back off to `$${1}3`, got `$conv2`")
  }

  test("isSupportedStringReplacePattern classifies regex patterns correctly") {
    val cases = Seq(
      "A" -> true,
      "A*" -> false,
      "(A)" -> false,
      "A+" -> false)

    cases.foreach { case (pattern, expected) =>
      assert(GpuOverrides.isSupportedStringReplacePattern(pattern) == expected)
    }
  }
}

/*
* This isn't actually a test.  It's just useful to help visualize what's going on when there are
* differences present.
*/
@Ignore
class StringOperatorsDiagnostics extends SparkQueryCompareTestSuite {
  def generateResults(gen : org.apache.spark.sql.Column => org.apache.spark.sql.Column):
      (Array[Row], Array[Row]) = {
    val (testConf, _) = setupTestConfAndQualifierName("", true, false,
      new SparkConf(), Seq.empty, 0.0, false)
    runOnCpuAndGpu(TestCodepoints.validCodepointCharsDF,
      frame => frame.select(gen(col("strings"))), testConf)
  }

  // utility function to print out detailed information on differences
  def generateUnicodeDiffs(title  : String,
      gen: () => (Array[Row], Array[Row])): Unit = {
    val (fromCpu, fromGpu) = gen()

    println(s"$title ----------------------------------------")

    println("\u001b[1;36mSummary of diffs:\u001b[0m")
    println("\u001b[1;36mCodepoint:\u001b[0m ")
    for (i <- fromCpu.indices) {
      if (fromCpu(i) != fromGpu(i)) {
        val codepoint = TestCodepoints.validCodepointIndices(i)
        print(f"$codepoint%5d, ")
      }
    }
    print("\n\n")

    println("\u001b[1;36mDetails:")
    println("Codepoint       CPU               GPU")
    println("single -> single mappings\u001b[0m");
    for (i <- fromCpu.indices) {
      if (fromCpu(i) != fromGpu(i) && fromCpu(i).getString(0).length == 1) {
        val codepoint = TestCodepoints.validCodepointIndices(i)

        print(f"(${codepoint.toChar.toString} $codepoint%5d[$codepoint%04x] " +
          f"(${fromCpu(i).getString(0)}")
        print(f"${fromCpu(i).getString(0)(0).toInt}%5d[${fromCpu(i).getString(0)(0).toInt}%04x]) ")
        println(f"${fromGpu(i).getString(0)(0).toInt}%5d[${fromGpu(i).getString(0)(0).toInt}%04x])")
      }
    }
    println("\u001b[1;36msingle -> multi mappings\u001b[0m");
    for (i <- fromCpu.indices) {
      if (fromCpu(i) != fromGpu(i) && fromCpu(i).getString(0).length > 1) {
        val cpu_str = fromCpu(i).getString(0)
        val gpu_str = fromGpu(i).getString(0)

        val codepoint = TestCodepoints.validCodepointIndices(i)
        print(f"(${codepoint.toChar.toString} $codepoint[$codepoint%04x]) ($cpu_str ")
        print(f"${cpu_str.map(c => "%d".format(c.toInt)).mkString(",")}")
        print("[")
        print(f"${cpu_str.map(c => "%04x".format(c.toInt)).mkString(",")}")
        print(f"]) ($gpu_str ")
        print(f"${gpu_str.map(c => "%d".format(c.toInt)).mkString(",")}")
        print("[");
        print(f"${gpu_str.map(c => "%04x".format(c.toInt)).mkString(",")}")
        println("])");
      }
    }
    println("---------------------------------------------")
  }
  // generateUnicodeDiffs("UPPER", () => generateResults(upper))
  // generateUnicodeDiffs("LOWER", () => generateResults(lower))

  // generates special case character mapping hash table generation input data.
  def generateCharMappings(): Unit = {
    class charMapping {
      var   num_upper = 0
      val   upper = Array(0, 0, 0)
      var   num_lower = 0
      val   lower = Array(0, 0, 0)
    }
    val mapping = Array.fill[charMapping](65536)(new charMapping())

    // upper results
    val (fromCpuUpper, fromGpuUpper) = generateResults(upper)
    for (i <- fromCpuUpper.indices) {
      if (fromCpuUpper(i) != fromGpuUpper(i) && fromGpuUpper(i).getString(0).length == 1) {
        val codepoint = TestCodepoints.validCodepointIndices(i)

        val cpu_str = fromCpuUpper(i).getString(0)
        mapping(codepoint).num_upper = cpu_str.length
        for (c <- 0 until cpu_str.length) { mapping(codepoint).upper(c) = cpu_str(c).toInt }
      }
    }

    // lower results
    val (fromCpuLower, fromGpuLower) = generateResults(lower)
    for (i <- fromCpuLower.indices) {
      if (fromCpuLower(i) != fromGpuLower(i) && fromGpuLower(i).getString(0).length == 1) {
        val codepoint = TestCodepoints.validCodepointIndices(i)

        val cpu_str = fromCpuLower(i).getString(0)
        mapping(codepoint).num_lower = cpu_str.length
        for (c <- 0 until cpu_str.length) { mapping(codepoint).lower(c) = cpu_str(c).toInt }
      }
    }

    // struct declaration
    println("struct special_case_mapping_in {")
    println("   uint16_t num_upper_chars;")
    println("   uint16_t upper[3];")
    println("   uint16_t num_lower_chars;")
    println("   uint16_t lower[3];")
    println("};")

    // mappings
    println("constexpr special_case_mapping_in codepoint_mapping_in[] = {")
    for (i <- 0 until 65536) {
      val mc = mapping(i)
      if (mc.num_upper != 0 || mc.num_lower != 0) {
        println(s"   { ${mc.num_upper} {${mc.upper(0)}, ${mc.upper(1)}, ${mc.upper(2)}}, " +
          s"${mc.num_lower}, {${mc.lower(0)}, ${mc.lower(1)}, ${mc.lower(2)}} },")
      }
    }
    println("};")

    // codepoints
    println("constexpr uint16_t codepoints_in[] = {\n")
    var count = 0
    for (i <- 0 until 65536) {
      val mc = mapping(i)
      if (mc.num_upper != 0 || mc.num_lower != 0) {
        print(s"   $i,")
        count = count + 1
        if (count > 0 && count % 10 == 0) {
          println("")
        }
      }
    }
    println("\n};")
  }
  // generateCharMappings()
}
