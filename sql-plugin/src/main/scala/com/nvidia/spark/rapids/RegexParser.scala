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

import java.sql.SQLException
import java.util.regex.{Pattern, PatternSyntaxException}

import scala.collection.mutable.ListBuffer

import com.nvidia.spark.rapids.GpuOverrides.regexMetaChars
import com.nvidia.spark.rapids.RegexParser.toReadableString

import org.apache.spark.unsafe.types.UTF8String

/**
 * Regular expression parser based on a Pratt Parser design.
 *
 * The goal of this parser is to build a minimal AST that allows us
 * to validate that we can support the expression on the GPU. The goal
 * is not to parse with the level of detail that would be required if
 * we were building an evaluation engine. For example, operator precedence is
 * largely ignored but could be added if we need it later.
 *
 * The Java and cuDF regular expression documentation has been used as a reference:
 *
 * Java regex: https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html
 * cuDF regex: https://docs.rapids.ai/api/libcudf/stable/md_regex.html
 *
 * The following blog posts provide some background on Pratt Parsers and parsing regex.
 *
 * - https://journal.stuffwithstuff.com/2011/03/19/pratt-parsers-expression-parsing-made-easy/
 * - https://matt.might.net/articles/parsing-regex-with-recursive-descent/
 */
class RegexParser(pattern: String) {
  // Note that [, ] and \ should be part of Punct, but they are handled separately
  private val regexPunct = """!"#$%&'()*+,-./:;<=>?@^_`{|}~"""
  private val escapeChars = Map('n' -> '\n', 'r' -> '\r', 't' -> '\t', 'f' -> '\f', 'a' -> '\u0007',
      'b' -> '\b', 'e' -> '\u001b')

  /** index of current position within the string being parsed */
  private var pos = 0

  def parse(): RegexAST = {
    // Validate if the pattern is compatible with Java, as this would throw an error otherwise
    Pattern.compile(pattern)

    val ast = parseUntil(() => eof())
    if (!eof()) {
      throw new RegexUnsupportedException("Failed to parse full regex. Last character parsed was",
        Some(pos))
    }
    ast
  }

  def parseReplacement(numCaptureGroups: Int): RegexReplacement = {
    val sequence = RegexReplacement(new ListBuffer(), numCaptureGroups)
    while (!eof()) {
      parseReplacementBase() match {
        case RegexSequence(parts) =>
          sequence.parts ++= parts
        case other =>
          sequence.parts += other
      }
    }
    sequence
  }

  private def parseReplacementBase(): RegexAST = {
      consume() match {
        case '\\' =>
          parseBackrefOrEscaped()
        case '$' =>
          parseBackrefOrLiteralDollar()
        case other =>
          RegexChar(other)
      }
  }


  private def parseUntil(until: () => Boolean): RegexAST = {
    val term = parseTerm(() => until() || peek().contains('|'))
    if (!eof() && peek().contains('|')) {
      consumeExpected('|')
      new RegexChoice(term, parseUntil(until), pos)
    } else {
      term
    }
  }

  private def parseTerm(until: () => Boolean): RegexAST = {
    val sequence = new RegexSequence(new ListBuffer(), pos)
    while (!eof() && !until()) {
      parseFactor(until) match {
        case RegexSequence(parts) =>
          sequence.parts ++= parts
        case other =>
          sequence.parts += other
      }
    }
    sequence
  }

  private def isValidQuantifierAhead(): Boolean = {
    if (peek().contains('{')) {
      val bookmark = pos
      consumeExpected('{')
      val q = parseQuantifierOrLiteralBrace()
      pos = bookmark
      q match {
        case _: QuantifierFixedLength | _: QuantifierVariableLength => true
        case _ => false
      }
    } else {
      false
    }
  }

  private def parseFactor(until: () => Boolean): RegexAST = {
    var start = pos
    var base = parseBase()
    base.position = Some(start)
    while (!eof() && !until()
        && (peek().exists(ch => ch == '*' || ch == '+' || ch == '?')
        || isValidQuantifierAhead())) {
      start = pos
      val quantifier = if (peek().contains('{')) {
        consumeExpected('{')
        parseQuantifierOrLiteralBrace().asInstanceOf[RegexQuantifier]
      } else {
        SimpleQuantifier(consume())
      }
      base = new RegexRepetition(base, quantifier, start)
      quantifier.position = Some(pos-1)
    }
    base
  }

  private def parseBase(): RegexAST = {
    val start = pos
    val base: RegexAST = consume() match {
      case '(' =>
        parseGroup()
      case '[' =>
        parseCharacterClass()
      case ']' =>
        RegexEscaped(']')
      case '}' =>
        RegexEscaped('}')
      case '\\' =>
        parseEscapedCharacter()
      case '\u0000' =>
        RegexGroup(false, RegexEscaped('0'), None)
      case '*' | '+' | '?' =>
        throw new RegexUnsupportedException(
          "Base expression cannot start with quantifier", Some(pos-1))
      case other =>
        RegexChar(other)
    }
    base.position = Some(start)
    base
  }

  private def parseGroup(): RegexAST = {
    var captureGroup = if (pos + 1 < pattern.length
        && pattern.charAt(pos) == '?'
        && pattern.charAt(pos+1) == ':') {
      pos += 2
      false
    } else {
      true
    }
    val lookahead = if (pos + 1 < pattern.length
        && "!=".contains(pattern.charAt(pos))) {
      pos += 1
      captureGroup = false
      pattern.charAt(pos-1) match {
        case '=' => Some(RegexPositiveLookahead)
        case '!' => Some(RegexNegativeLookahead)
      }
    } else {
      None
    }
    val term = parseUntil(() => peek().contains(')'))
    consumeExpected(')')
    RegexGroup(captureGroup, term, lookahead)
  }

  private def parseCharacterClass(): RegexCharacterClass = {
    val supportedMetaCharacters = "\\^-[]+."

    def getEscapedComponent(): RegexCharacterClassComponent = {
      peek() match {
        case Some('x') =>
          consumeExpected('x')
          val hexChar = parseHexDigit
          hexChar.codePoint match {
            case 0 => hexChar
            case codePoint => RegexChar(codePoint.toChar)
          }
        case Some('0') =>
          val octalChar = parseOctalDigit
          octalChar.codePoint match {
            case 0 => RegexHexDigit("00")
            case codePoint => RegexChar(codePoint.toChar)
          }
        case Some(ch) =>
          consumeExpected(ch) match {
            // NOTE: Should switch to ASCII mode to simplify and expand this fix
            case 'd' => RegexCharacterRange(RegexChar('0'), RegexChar('9'))
            // List of character literals with an escape from here, under "Characters"
            // https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html
            case ch if escapeChars.contains(ch) =>
              RegexChar(escapeChars(ch))
            case ch =>
              if (supportedMetaCharacters.contains(ch)) {
                // an escaped metacharacter ('\\', '^', '-', ']', '+')
                RegexEscaped(ch)
              } else {
                throw new RegexUnsupportedException(
                  s"Unsupported escaped character '$ch' in character class", Some(pos-1))
              }
          }
        case None =>
          throw new RegexUnsupportedException(
                s"Unclosed character class", Some(pos))
      }
    }

    val start = pos
    val characterClass = new RegexCharacterClass(negated = false, characters = ListBuffer(), pos)
    // loop until the end of the character class or EOF
    var characterClassComplete = false
    while (!eof() && !characterClassComplete) {
      val ch = consume()
      ch match {
        case '[' =>
          // treat as a literal character and add to the character class
          characterClass.append(new RegexChar(ch, pos-1))
        case ']' if (!characterClass.negated && pos > start + 1) ||
            (characterClass.negated && pos > start + 2) =>
          // "[]" is not a valid character class
          // "[]a]" is a valid character class containing the characters "]" and "a"
          // "[^]a]" is a valid negated character class containing the characters "]" and "a"
          characterClassComplete = true
        case '^' if pos == start + 1 =>
          // Negates the character class, causing it to match a single character not listed in
          // the character class. Only valid immediately after the opening '['
          characterClass.negated = true
        case ch =>
          val nextChar: RegexCharacterClassComponent = ch match {
            case '\\' =>
              getEscapedComponent() match {
                case RegexChar(ch) if supportedMetaCharacters.contains(ch) =>
                  // A hex or octal representation of a meta character gets treated as an escaped
                  // char. Example: [\x5ea] is treated as [\^a], not just [^a]
                  RegexEscaped(ch)
                case other => other
              }
            case '&' =>
              peek() match {
                case Some('&') =>
                  throw new RegexUnsupportedException("" +
                    "cuDF does not support class intersection operator &&", Some(pos-1))
                case _ => // ignore
              }
              RegexChar('&')
            case '\u0000' =>
              RegexHexDigit("00")
            case ch =>
              RegexChar(ch)
          }
          nextChar.position = Some(pos-1)
          peek() match {
            case Some('-') =>
              consumeExpected('-')
              peek() match {
                case Some(']') =>
                  // '-' at end of class e.g. "[abc-]"
                  characterClass.append(nextChar)
                  characterClass.append('-')
                case Some('\\') =>
                  consumeExpected('\\')
                  characterClass.appendRange(nextChar, getEscapedComponent())
                case Some(end) =>
                  skip()
                  characterClass.appendRange(nextChar, RegexChar(end))
                case _ =>
                  throw new RegexUnsupportedException(
                    "Unexpected EOF while parsing character range", Some(pos))
              }
            case _ =>
              characterClass.append(nextChar)
          }
      }
    }
    if (!characterClassComplete) {
      throw new RegexUnsupportedException(s"Unclosed character class", Some(pos))
    }
    characterClass
  }


  /**
   * Parse a quantifier in one of the following formats:
   *
   * {n}
   * {n,}
   * {n,m} (only valid if m >= n)
   */
  private def parseQuantifierOrLiteralBrace(): RegexAST = {

    // assumes that '{' has already been consumed
    val start = pos

    def treatAsLiteralBrace() = {
      // this was not a quantifier, just a literal '{'
      pos = start + 1
      RegexChar('{')
    }

    consumeInt match {
      case Some(minLength) =>
        peek() match {
          case Some(',') =>
            consumeExpected(',')
            val max = consumeInt()
            if (peek().contains('}')) {
              consumeExpected('}')
              max match {
                case None =>
                  QuantifierVariableLength(minLength, None)
                case Some(m) =>
                  if (m >= minLength) {
                    QuantifierVariableLength(minLength, max)
                  } else {
                    treatAsLiteralBrace()
                  }
              }
            } else {
              treatAsLiteralBrace()
            }
          case Some('}') =>
            consumeExpected('}')
            QuantifierFixedLength(minLength)
          case _ =>
            treatAsLiteralBrace()
        }
      case None =>
        treatAsLiteralBrace()
    }
  }

  private def parseBackrefOrEscaped(): RegexAST = {
    val start = pos

    consumeInt match {
      case Some(refNum) =>
        RegexBackref(refNum)
      case None =>
        pos = start
        RegexChar('\\')
    }
  }

  private def parseBackrefOrLiteralDollar(): RegexAST = {
    val start = pos

    def treatAsLiteralDollar() = {
      pos = start
      RegexChar('$')
    }

    peek() match {
      case Some('{') =>
        consumeExpected('{')
        val num = consumeInt()
        if (peek().contains('}')) {
          consumeExpected('}')
          num match {
            case Some(_) =>
              throw new RegexUnsupportedException(
                "Numeric `${N}` backref in replacement string is not supported on GPU " +
                  "(Java's Matcher.appendReplacement rejects this syntax)",
                Some(start))
            case _ =>
              treatAsLiteralDollar()
          }
        } else {
          treatAsLiteralDollar()
        }
      case Some(ch) if ch >= '1' && ch <= '9' =>
        val num = consumeInt()
        num match {
          case Some(n) =>
            RegexBackref(n)
          case _ =>
            treatAsLiteralDollar()
        }
      case _ =>
        treatAsLiteralDollar()
    }
  }

  private def parseEscapedCharacter(): RegexAST = {
    peek() match {
      case None =>
        throw new RegexUnsupportedException("Pattern may not end with trailing escape", Some(pos))
      case Some(ch) =>
        ch match {
          case 'A' | 'Z' | 'z' =>
            // string anchors
            consumeExpected(ch)
            RegexEscaped(ch)
          case 's' | 'S' | 'd' | 'D' | 'w' | 'W' | 'v' | 'V' | 'h' | 'H' | 'R' =>
            // meta sequences
            consumeExpected(ch)
            RegexEscaped(ch)
          case 'B' | 'b' =>
            // word boundaries
            consumeExpected(ch)
            RegexEscaped(ch)
          case '[' | ']' | '\\' | '^' | '$' | '.' | '|' | '?' | '*' | '+' | '(' | ')' | '{' | '}' =>
            // escaped metacharacter
            consumeExpected(ch)
            RegexEscaped(ch)
          case 'x' =>
            consumeExpected(ch)
            parseHexDigit
          case '0' =>
            parseOctalDigit
          case 'p' | 'P' =>
            parsePredefinedClass
          case _ if escapeChars.contains(ch) =>
            consumeExpected(ch)
            RegexChar(escapeChars(ch))
          case _ if regexPunct.contains(ch) =>
            // other punctuation
            // note that this may include metacharacters from earlier, this is just to
            // handle characters not covered by the previous cases earlier
            consumeExpected(ch)
            RegexEscaped(ch)
          case other =>
            throw new RegexUnsupportedException(
              s"Invalid or unsupported escape character '$other'", Some(pos - 1))
        }
    }
  }

  private def parsePredefinedClass: RegexCharacterClass = {
    val negated = consume().isUpper
    consumeExpected('{')
    val start = pos
    while(!eof() && pattern.charAt(pos).isLetter) {
      pos += 1
    }
    val className = pattern.substring(start, pos)
    def getCharacters(className: String): ListBuffer[RegexCharacterClassComponent] = {
      // Character lists from here:
      // https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html
      className match {
        case "Lower" =>
          ListBuffer(RegexCharacterRange(RegexChar('a'), RegexChar('z')))
        case "Upper" =>
          ListBuffer(RegexCharacterRange(RegexChar('A'), RegexChar('Z')))
        case "ASCII" =>
          ListBuffer(RegexCharacterRange(RegexHexDigit("00"), RegexChar('\u007f')))
        case "Alpha" =>
          ListBuffer(getCharacters("Lower"), getCharacters("Upper")).flatten
        case "Digit" =>
          ListBuffer(RegexCharacterRange(RegexChar('0'), RegexChar('9')))
        case "Alnum" =>
          ListBuffer(getCharacters("Alpha"), getCharacters("Digit")).flatten
        case "Punct" =>
          val res:ListBuffer[RegexCharacterClassComponent] =
              ListBuffer(regexPunct.map(RegexChar): _*)
          res ++= ListBuffer(RegexEscaped('['), RegexEscaped(']'), RegexEscaped('\\'))
        case "Graph" =>
          ListBuffer(getCharacters("Alnum"), getCharacters("Punct")).flatten
        case "Print" =>
          val res = getCharacters("Graph")
          res += RegexChar('\u0020')
        case "Blank" =>
          ListBuffer(RegexChar(' '), RegexEscaped('t'))
        case "Cntrl" =>
          ListBuffer(RegexCharacterRange(RegexHexDigit("00"), RegexChar('\u001f')),
            RegexChar('\u007f'))
        case "XDigit" =>
          ListBuffer(RegexCharacterRange(RegexChar('0'), RegexChar('9')),
            RegexCharacterRange(RegexChar('a'), RegexChar('f')),
            RegexCharacterRange(RegexChar('A'), RegexChar('F')))
        case "Space" =>
          ListBuffer(" \t\n\u000B\f\r".map(RegexChar): _*)
        case _ =>
          throw new RegexUnsupportedException(
            s"Predefined character class ${className} is not supported", Some(start))
      }
    }
    consumeExpected('}')
    RegexCharacterClass(negated, characters = getCharacters(className))
  }

  private def isHexDigit(ch: Char): Boolean = ch.isDigit ||
    (ch >= 'a' && ch <= 'f') ||
    (ch >= 'A' && ch <= 'F')

  private def parseHexDigit: RegexHexDigit = {
    // \xhh      The character with hexadecimal value 0xhh
    // \x{h...h} The character with hexadecimal value 0xh...h
    //           (Character.MIN_CODE_POINT  <= 0xh...h <=  Character.MAX_CODE_POINT)

    val varHex = pattern.charAt(pos) == '{'
    if (varHex) {
      consumeExpected('{')
    }
    val start = pos
    // Java spec: non-braced `\xNN` consumes EXACTLY two hex digits and any
    // trailing hex digits are part of the surrounding pattern. The braced form
    // `\x{h...h}` consumes hex digits until the closing '}' (validated below).
    // Issue #14739: previously this loop was unbounded for both forms, so
    // patterns like "\\x61a" were rejected even though they are valid Java
    // regex meaning "0x61 (= 'a') followed by the literal 'a'".
    val hexLimit = if (varHex) Int.MaxValue else 2
    while (pos - start < hexLimit && !eof() && isHexDigit(pattern.charAt(pos))) {
      pos += 1
    }
    val hexDigit = pattern.substring(start, pos)
    if (varHex) {
      consumeExpected('}')
    } else if (hexDigit.length != 2) {
      throw new RegexUnsupportedException(s"Invalid hex digit: $hexDigit", Some(start))
    }

    val value = Integer.parseInt(hexDigit, 16)
    if (value < Character.MIN_CODE_POINT || value > Character.MAX_CODE_POINT) {
      throw new RegexUnsupportedException(s"Invalid hex digit: $hexDigit", Some(start))
    }
    new RegexHexDigit(hexDigit, start - 2)
  }

  private def isOctalDigit(ch: Char): Boolean = ch >= '0' && ch <= '7'

  private def parseOctalDigit: RegexOctalChar = {
    // \0n   The character with octal value 0n (0 <= n <= 7)
    // \0nn  The character with octal value 0nn (0 <= n <= 7)
    // \0mnn The character with octal value 0mnn (0 <= m <= 3, 0 <= n <= 7)

    def parseOctalDigits(n: Integer): RegexOctalChar = {
      val octal = pattern.substring(pos, pos + n)
      pos += n
      new RegexOctalChar(octal, pos)
    }

    if (!eof() && isOctalDigit(pattern.charAt(pos))) {
      if (pos + 1 < pattern.length && isOctalDigit(pattern.charAt(pos + 1))) {
        if (pos + 2 < pattern.length && isOctalDigit(pattern.charAt(pos + 2))
            && pattern.charAt(pos) <= '3') {
          if (pos + 3 < pattern.length && isOctalDigit(pattern.charAt(pos + 3))
              && pattern.charAt(pos+1) <= '3' && pattern.charAt(pos) == '0') {
            parseOctalDigits(4)
          } else {
            parseOctalDigits(3)
          }
        } else {
          parseOctalDigits(2)
        }
      } else {
        parseOctalDigits(1)
      }
    } else {
      throw new RegexUnsupportedException(
        "Invalid octal digit", Some(pos))
    }
  }

  /** Determine if we are at the end of the input */
  private def eof(): Boolean = pos == pattern.length

  /** Advance the index by one */
  private def skip(): Unit = {
    if (eof()) {
      throw new RegexUnsupportedException("Unexpected EOF", Some(pos))
    }
    pos += 1
  }

  /** Get the next character and advance the index by one */
  private def consume(): Char = {
    if (eof()) {
      throw new RegexUnsupportedException("Unexpected EOF", Some(pos))
    } else {
      pos += 1
      pattern.charAt(pos - 1)
    }
  }

  /** Consume the next character if it is the one we expect */
  private def consumeExpected(expected: Char): Char = {
    val consumed = consume()
    if (consumed != expected) {
      throw new RegexUnsupportedException(
        s"Expected '$expected' but found '$consumed'", Some(pos-1))
    }
    consumed
  }

  /** Peek at the next character without consuming it */
  private def peek(): Option[Char] = {
    if (eof()) {
      None
    } else {
      Some(pattern.charAt(pos))
    }
  }

  private def consumeInt(): Option[Int] = {
    val start = pos
    while (!eof() && peek().exists(_.isDigit)) {
      skip()
    }
    if (start == pos) {
      None
    } else {
      Some(pattern.substring(start, pos).toInt)
    }
  }

}

object RegexParser {
  private val regexpChars = Set('\u0000', '\\', '.', '^', '$', '\u0007', '\u001b', '\f')

  def parse(pattern: String): RegexAST = new RegexParser(pattern).parse

  def isRegExpString(s: String): Boolean = {

    def isRegExpString(ast: RegexAST): Boolean = ast match {
      case RegexChar(ch) => regexpChars.contains(ch)
      case RegexEscaped(_) => true
      case RegexSequence(parts) => parts.exists(isRegExpString)
      case _ => true
    }

    try {
      val parser = new RegexParser(s)
      val ast = parser.parse()
      isRegExpString(ast)
    } catch {
      case _: RegexUnsupportedException | _: PatternSyntaxException =>
        // if we cannot parse it then assume that it might be valid regexp
        true
    }
  }

  def toReadableString(x: String): String = {
    x.map {
      case '\r' => "\\r"
      case '\n' => "\\n"
      case '\t' => "\\t"
      case '\f' => "\\f"
      case '\u0000' => "\\u0000"
      case '\u000b' => "\\u000b"
      case '\u0085' => "\\u0085"
      case '\u2028' => "\\u2028"
      case '\u2029' => "\\u2029"
      case other => other
    }.mkString
  }

}

sealed trait RegexMode
object RegexFindMode extends RegexMode
object RegexReplaceMode extends RegexMode
object RegexSplitMode extends RegexMode

sealed trait RegexLookahead
object RegexNegativeLookahead extends RegexLookahead
object RegexPositiveLookahead extends RegexLookahead

sealed class RegexRewriteFlags(val emptyRepetition: Boolean)

/**
 * Transpile Java/Spark regular expression to a format that cuDF supports, or throw an exception
 * if this is not possible.
 *
 * @param mode  RegexFindMode    if matching only (rlike)
                RegexReplaceMode if performing a replacement (regexp_replace)
                RegexSplitMode   if performing a split (string_split)
 */
class CudfRegexTranspiler(mode: RegexMode) {
  private val regexPunct = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"
  private val escapeChars = Map('n' -> '\n', 'r' -> '\r', 't' -> '\t', 'f' -> '\f', 'a' -> '\u0007',
      'b' -> '\b', 'e' -> '\u001b')

  private def countCaptureGroups(regex: RegexAST): Int = {
    regex match {
      case RegexSequence(parts) => parts.foldLeft(0)((c, re) => c + countCaptureGroups(re))
      case RegexGroup(capture, base, _) =>
        if (capture) {
          1 + countCaptureGroups(base)
        } else {
          countCaptureGroups(base)
        }
      case _ => 0
    }
  }

  /**
   * Parse Java regular expression and translate into cuDF regular expression.
   *
   * @param pattern Regular expression that is valid in Java's engine
   * @param extractIndex extraction index for regular expression
   * @param repl Optional replacement pattern
   * @return Regular expression and optional replacement in cuDF format
   */
  def transpile(pattern: String, extractIndex: Option[Int], repl: Option[String]):
        (String, Option[String]) = {
    val (cudfRegex, replacement) = getTranspiledAST(pattern, extractIndex, repl)

    // write out to regex string, performing minor transformations
    // such as adding additional escaping
    (cudfRegex.toRegexString, replacement.map(_.toRegexString))
  }

  def getTranspiledAST(
      regex: RegexAST,
      extractIndex: Option[Int],
      repl: Option[String]): (RegexAST, Option[RegexReplacement]) = {

    // if we have a replacement, parse the replacement string using the regex parser to account
    // for backrefs
    val replacement = repl.map(s => new RegexParser(s).parseReplacement(countCaptureGroups(regex)))

    // validate that the regex is supported by cuDF
    val cudfRegex = transpile(regex, extractIndex, replacement, None)

    (cudfRegex, replacement)
  }

  /**
   * Parse Java regular expression and translate into cuDF regular expression in AST form.
   *
   * @param pattern Regular expression that is valid in Java's engine
   * @param extractIndex extraction index for regular expression
   * @param repl Optional replacement pattern
   * @return Regular expression AST and optional replacement in cuDF format
   */
  def getTranspiledAST(
      pattern: String,
      extractIndex: Option[Int],
      repl: Option[String]): (RegexAST, Option[RegexReplacement]) = {
    // parse the source regular expression
    val regex = new RegexParser(pattern).parse()
    getTranspiledAST(regex, extractIndex, repl)
  }

  def transpileToSplittableString(e: RegexAST): Option[String] = {
    e match {
      case RegexEscaped('b') | RegexEscaped('B') => None
      case RegexEscaped(ch) if escapeChars.contains(ch) => Some(escapeChars(ch).toString)
      case RegexEscaped(ch) if regexPunct.contains(ch) => Some(ch.toString)
      case RegexChar(ch) if !regexMetaChars.contains(ch) => Some(ch.toString)
      case RegexSequence(parts) =>
        parts.foldLeft[Option[String]](Some("")) { (all, x) =>
          all match {
            case Some(current) =>
              transpileToSplittableString(x) match {
                case Some(y) => Some(current + y)
                case _ => None
              }
            case _ => None
          }
        }
      case _ => None
    }
  }

  def transpileToSplittableString(pattern: String): Option[String] = {
    try {
      val regex = new RegexParser(pattern).parse()
      transpileToSplittableString(regex)
    } catch {
      // treat as regex if we can't parse it
      case _: RegexUnsupportedException =>
        None
    }
  }

  @scala.annotation.tailrec
  private def isRepetition(e: RegexAST, checkZeroLength: Boolean): Boolean = {
    e match {
      case RegexRepetition(_, _) if !checkZeroLength => true
      case RegexRepetition(_, quantifier) => quantifier match {
        case SimpleQuantifier(ch) if "*?".contains(ch) => true
        case QuantifierFixedLength(length) if length == 0 => true
        case QuantifierVariableLength(min, _) if min == 0 => true
        case _ => false
      }
      case RegexGroup(_, term, _) => isRepetition(term, checkZeroLength)
      case RegexSequence(parts) if parts.nonEmpty => isRepetition(parts.last, checkZeroLength)
      case _ => false
    }
  }

  private def getUnsupportedRepetitionBaseOption(e: RegexAST): Option[RegexAST] = {
    e match {
      case RegexEscaped(ch) => ch match {
        case 'd' | 'w' | 's' | 'S' | 'h' | 'H' | 'v' | 'V' => None
        case _ => Some(e)
      }
      case RegexChar(a) if "$^".contains(a) =>
        // example: "$*"
        Some(e)

      case RegexRepetition(_, _) =>
        // example: "a*+"
        Some(e)

      case RegexSequence(parts) =>
        parts.foreach { part => getUnsupportedRepetitionBaseOption(part) match {
            case r @ Some(_) => return r
            case None =>
          }
        }
        None

      case RegexChoice(a, b) =>
        getUnsupportedRepetitionBaseOption(a)
          .orElse(getUnsupportedRepetitionBaseOption(b))

      case RegexGroup(_, term, _) =>
        getUnsupportedRepetitionBaseOption(term)

      case _ => None
    }
  }

  private def getUnsupportedRepetitionBase(e: RegexAST): RegexAST = {
    getUnsupportedRepetitionBaseOption(e) match {
      case None => throw new NoSuchElementException(
        s"Expected repetition base ${e.toRegexString} to be unsupported but was actully supported")
      case Some(unsupportedTerm) => unsupportedTerm
    }
  }

  private def isSupportedRepetitionBase(e: RegexAST): Boolean = {
    getUnsupportedRepetitionBaseOption(e) match {
      case None => true
      case _ => false
    }
  }

  private val lineTerminatorChars = Seq('\n', '\r', '\u0085', '\u2028', '\u2029')


  private def negateCharacterClass(
      components: ListBuffer[RegexCharacterClassComponent]): RegexAST = {
    // There are differences between cuDF and Java handling of `\r`
    // in negated character classes. The expression `[^a]` will match
    // `\r` in Java but not in cuDF, so we replace `[^a]` with
    // `(?:[\r]|[^a])`.
    //
    // Examples:
    //
    // `[^a]`     => `(?:[\r]|[^a])`
    // `[^a\n]`   => `(?:[\r]|[^a\n])`
    //
    // If the negated character class contains `\r` then there is no transformation:
    //
    // `[^a\r]`   => `[^a\r]`
    // `[^a\r\n]` => `[^a\r\n]`

    val componentsWithoutLinefeed = components.filterNot {
      case RegexChar(ch) => ch == '\r'
      case RegexEscaped(ch) => ch == 'r'
      case RegexCharacterRange(startRegex, RegexChar(end)) =>
        val start = startRegex match {
          case RegexChar(ch) => ch
          case r @ RegexOctalChar(_) => r.codePoint.toChar
          case r @ RegexHexDigit(_) => r.codePoint.toChar
          case other => throw new RegexUnsupportedException(
            s"Unexpected expression at start of character range: ${other.toRegexString}",
            other.position)
        }
        start <= '\r' && end >= '\r'
      case _ =>
        false
    }

    if (componentsWithoutLinefeed.length != components.length) {
      // no modification needed in this case
      RegexCharacterClass(negated = true, ListBuffer(components.toSeq: _*))
    } else {
      RegexGroup(capture = false,
        RegexChoice(
          RegexCharacterClass(negated = false,
            characters = ListBuffer(RegexChar('\r'))),
          RegexCharacterClass(negated = true, ListBuffer(components.toSeq: _*))), None)
    }
  }

  private def transpile(regex: RegexAST, extractIndex: Option[Int],
      replacement: Option[RegexReplacement],
      previous: Option[RegexAST]): RegexAST = {

    def containsBeginAnchor(regex: RegexAST): Boolean = {
      contains(regex, {
        case RegexChar('^') | RegexEscaped('A') => true
        case _ => false
      }, recurseIntoCharacterClasses = false)
    }

    def containsEndAnchor(regex: RegexAST): Boolean = {
      contains(regex, {
        case RegexChar('$') | RegexEscaped('z') | RegexEscaped('Z') => true
        case _ => false
      }, recurseIntoCharacterClasses = false)
    }

    def containsNewline(regex: RegexAST): Boolean = {
      contains(regex, {
        case RegexChar('\r') | RegexEscaped('r') => true
        case RegexChar('\n') | RegexEscaped('n') => true
        case RegexChar('\u0085') | RegexChar('\u2028') | RegexChar('\u2029') => true
        case RegexEscaped('s') | RegexEscaped('v') | RegexEscaped('R') => true
        case RegexEscaped('W') | RegexEscaped('D') |
          RegexEscaped('S') | RegexEscaped('V') =>
          // these would get transpiled to negated character classes
          // that include newlines
          true
        case RegexCharacterClass(true, _) => true
        case _ => false
      })
    }

    def containsEmpty(regex: RegexAST): Boolean = {
      contains(regex, {
        case RegexRepetition(_, term) => term match {
          case SimpleQuantifier('*') | SimpleQuantifier('?') => true
          case QuantifierFixedLength(0) => true
          case QuantifierVariableLength(0, _) => true
          case _ => false
        }
        case _ => false
      })
    }

    // check a pair of regex ast nodes for unsupported combinations
    // of end string/line anchors and newlines or optional items
    def checkEndAnchorContext(r1: RegexAST, r2: RegexAST): Unit = {
      if ((containsEndAnchor(r1) &&
          (containsNewline(r2) || containsEmpty(r2) || containsBeginAnchor(r2))) ||
        (containsEndAnchor(r2) &&
          (containsNewline(r1) || containsBeginAnchor(r1)))) {
        throw new RegexUnsupportedException(
          s"End of line/string anchor is not supported in this context: " +
            s"${toReadableString(r1.toRegexString)}" +
            s"${toReadableString(r2.toRegexString)}", r1.position)
      }
    }

    def checkAdjacentEndAnchorContextSplit(r1: RegexAST, r2: RegexAST): Unit = {
      if ((containsEndAnchor(r1) &&
          (containsNewline(r2) || containsEmpty(r2) || containsBeginAnchor(r2))) ||
        (containsEndAnchor(r2) &&
          (containsNewline(r1) || containsEmpty(r1) || containsBeginAnchor(r1)))) {
        throw new RegexUnsupportedException(
          s"End of line/string anchor is not supported in this context: " +
            s"${toReadableString(r1.toRegexString)}" +
            s"${toReadableString(r2.toRegexString)}", r1.position)
      }
    }

    def checkEndAnchorContextSplit(parts: Seq[RegexAST]): Unit = {
      var prefixContainsEmpty = false
      var previousPart: Option[RegexAST] = None

      parts.zipWithIndex.foreach { case (part, index) =>
        previousPart.foreach(previous => checkAdjacentEndAnchorContextSplit(previous, part))

        // Split also rejects an optional term anywhere before a real end anchor. Keep this
        // prefix-wide check limited to optional terms: newline and begin-anchor context is
        // intentionally adjacent-only, so supported expressions such as ^a$ and \na$ remain
        // on the GPU.
        if (prefixContainsEmpty && containsEndAnchor(part)) {
          val prefix = parts.take(index).map(_.toRegexString).mkString
          throw new RegexUnsupportedException(
            s"End of line/string anchor is not supported in this context: " +
              s"${toReadableString(prefix)}${toReadableString(part.toRegexString)}",
            part.position)
        }

        prefixContainsEmpty = prefixContainsEmpty || containsEmpty(part)
        previousPart = Some(part)
      }
    }

    def checkUnsupported(regex: RegexAST): Unit = {
      regex match {
        case RegexSequence(parts) if mode == RegexSplitMode =>
          checkEndAnchorContextSplit(parts.toSeq)
        case RegexSequence(parts) =>
          for (i <- 1 until parts.length) {
            checkEndAnchorContext(parts(i - 1), parts(i))
          }
        case RegexChoice(l, r) =>
          checkUnsupported(l)
          checkUnsupported(r)
        case RegexGroup(_, term, _) => checkUnsupported(term)
        case RegexRepetition(ast, _) => checkUnsupported(ast)
        case _ =>
          // ignore
      }
    }

    def isEmptyRepetition(regex: RegexAST): Boolean = {
      regex match {
        case RegexRepetition(_, term) => term match {
          case SimpleQuantifier('*') | SimpleQuantifier('?') => true
          case QuantifierFixedLength(0) => true
          case QuantifierVariableLength(0, _) => true
          case _ => false
        }
        case RegexGroup(_, term, _) =>
          isEmptyRepetition(term)
        case RegexSequence(parts) =>
          parts.forall(isEmptyRepetition)
        case RegexChoice(l, r) =>
          isEmptyRepetition(l) || isEmptyRepetition(r)
        case _ => false
      }
    }

    checkUnsupported(regex)

    var current = 0
    // capture groups can be nested, so we need to do this logic outside of the rewrite
    def updateGroupsForExtract(regex: RegexAST, n: Int): RegexAST = {
      regex match {
        case RegexGroup(capture, term, lookahead) if capture => {
          current += 1
          RegexGroup(n == current, updateGroupsForExtract(term, n), lookahead)
        }
        case RegexGroup(false, term, lookahead) =>
          RegexGroup(capture = false, updateGroupsForExtract(term, n), lookahead)
        case RegexSequence(parts) =>
          RegexSequence(parts.map(updateGroupsForExtract(_, n)))
        case RegexChoice(left, right) =>
          RegexChoice(updateGroupsForExtract(left, n), updateGroupsForExtract(right, n))
        case RegexRepetition(term, quantifier) =>
          RegexRepetition(updateGroupsForExtract(term, n), quantifier)
        case _ => regex
      }
    }

    val withUpdatedGroups = extractIndex match {
      case Some(n) =>
        updateGroupsForExtract(regex, n)
      case _ => regex
    }

    val flags = new RegexRewriteFlags(isEmptyRepetition(regex))

    rewrite(withUpdatedGroups, replacement, previous, flags)
  }

  private def rewrite(regex: RegexAST, replacement: Option[RegexReplacement],
      previous: Option[RegexAST], flags: RegexRewriteFlags): RegexAST = {
    regex match {

      case RegexChar(ch) => ch match {
        case '.' =>
          // workaround for https://github.com/rapidsai/cudf/issues/9619
          val terminatorChars = new ListBuffer[RegexCharacterClassComponent]()
          terminatorChars ++= lineTerminatorChars.map(RegexChar)
          RegexCharacterClass(negated = true, terminatorChars)
        case '$' if mode == RegexSplitMode =>
          RegexEscaped('Z')
        case '$' =>
          // in the case of the line anchor $, the JVM has special conditions when handling line
          // terminators in and around the anchor
          // this handles cases where the line terminator characters are *before* the anchor ($)
          // NOTE: this applies to when using *standard* mode. In multiline mode, all these
          // conditions will change. Currently Spark does not use multiline mode.
          previous match {
            case Some(RegexChar('$')) | Some(RegexEscaped('Z')) =>
              // repeating the line anchor in cuDF (for example b$$) causes matches to fail, but in
              // Java, it's treated as a single (b$ and b$$ are synonymous), so we create
              // an empty RegexAST that outputs to empty string
              RegexEmpty()
            case Some(RegexChar(ch)) if mode == RegexReplaceMode
                && lineTerminatorChars.contains(ch) =>
                throw new RegexUnsupportedException("Regex sequences with a line terminator "
                    + "character followed by '$' are not supported in replace mode", regex.position)
            case Some(RegexEscaped('b')) | Some(RegexEscaped('B')) =>
              throw new RegexUnsupportedException(
                      "Regex sequences with \\b or \\B not supported around $", regex.position)
            case _ =>
              // cuDF #22763 makes EXT_NEWLINE treat \r\n as one line terminator for $, so emit
              // the anchor directly instead of the (\r\n)? synthetic group it needed before.
              RegexChar('$')
          }
        case '^' if mode == RegexSplitMode =>
          RegexEscaped('A')
        case '\r' | '\n' if mode == RegexFindMode =>
          previous match {
            case Some(RegexChar('$')) =>
              RegexEmpty()
            case _ =>
              regex
          }
        case _ =>
          regex
      }

      case r @ RegexOctalChar(digits) =>
        val octal = if (digits.charAt(0) == '0' && digits.length == 4) {
          digits.substring(1)
        } else  {
          digits
        }

        if (regexMetaChars.map(_.toInt).contains(r.codePoint)) {
          RegexEscaped(r.codePoint.toChar)
        } else if(r.codePoint >= 128) {
          RegexChar(r.codePoint.toChar)
        } else {
          RegexOctalChar(octal)
        }

      case r @ RegexHexDigit(_) =>
        if (regexMetaChars.map(_.toInt).contains(r.codePoint)) {
          RegexEscaped(r.codePoint.toChar)
        } else if (r.codePoint >= 128) {
          // cuDF only supports 0x00 to 0x7f hexidecimal chars
          RegexChar(r.codePoint.toChar)
        } else {
          RegexHexDigit(String.format("%02x", Int.box(r.codePoint)))
        }

      case RegexEscaped(ch) => ch match {
        case 'd' | 'D' =>
          // cuDF is not compatible with Java for \d  so we transpile to Java's definition
          // of [0-9]
          // https://github.com/rapidsai/cudf/issues/10894
          val components = ListBuffer[RegexCharacterClassComponent](
            RegexCharacterRange(RegexChar('0'), RegexChar('9')))
          if (ch.isUpper) {
            negateCharacterClass(components)
          } else {
            RegexCharacterClass(negated = false, components)
          }
        case 'w' | 'W' =>
          // cuDF is not compatible with Java for \w so we transpile to Java's definition
          // of `[a-zA-Z_0-9]`
          val components = ListBuffer[RegexCharacterClassComponent](
            RegexCharacterRange(RegexChar('a'), RegexChar('z')),
            RegexCharacterRange(RegexChar('A'), RegexChar('Z')),
            RegexChar('_'),
            RegexCharacterRange(RegexChar('0'), RegexChar('9')))
          if (ch.isUpper) {
            negateCharacterClass(components)
          } else {
            RegexCharacterClass(negated = false, components)
          }
        case 'b' | 'B' if mode == RegexSplitMode =>
          // see https://github.com/NVIDIA/spark-rapids/issues/5478
          throw new RegexUnsupportedException(
              "Word boundaries are not supported in split mode", regex.position)
        case 'b' | 'B' =>
          previous match {
            case Some(RegexEscaped(ch)) if "DWSHV".contains(ch) =>
              throw new RegexUnsupportedException(
                  "Word boundaries around \\D, \\S,\\W, \\H, or \\V are not supported",
                  regex.position)
            case Some(RegexCharacterClass(negated, _)) if negated =>
              throw new RegexUnsupportedException(
                  "Word boundaries around negated character classes are not supported",
                  regex.position)
            case _ =>
              RegexEscaped(ch)
          }
        case 'z' if mode == RegexSplitMode =>
          RegexEscaped('Z')
        case 'z' =>
          // cuDF does not support "\z" except for in split mode
          throw new RegexUnsupportedException(
            "\\z is not supported on GPU for find or replace",
            regex.position)
        case 'Z' =>
          // \Z is really a synonymn for $. It's used in Java to preserve that behavior when
          // using modes that change the meaning of $ (such as MULTILINE or UNIX_LINES)
          previous match {
            case Some(RegexEscaped('Z')) =>
              RegexEmpty()
            case _ =>
              rewrite(RegexChar('$'), replacement, previous, flags)
          }
        case 's' | 'S' =>
          // whitespace characters
          val chars: ListBuffer[RegexCharacterClassComponent] = ListBuffer(
            RegexChar(' '), RegexChar('\u000b'))
          chars ++= Seq('n', 't', 'r', 'f').map(RegexEscaped)
          if (ch.isUpper) {
            negateCharacterClass(chars)
          } else {
            RegexCharacterClass(negated = false, characters = chars)
          }
        case 'h' | 'H' =>
          // horizontal whitespace
          // see https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html
          // under "Predefined character classes"
          val chars: ListBuffer[RegexCharacterClassComponent] = ListBuffer(
            RegexChar(' '), RegexChar('\u00A0'), RegexChar('\u1680'), RegexChar('\u180e'),
            RegexChar('\u202f'), RegexChar('\u205f'), RegexChar('\u3000')
          )
          chars += RegexEscaped('t')
          chars += RegexCharacterRange(RegexChar('\u2000'), RegexChar('\u200a'))
          if (ch.isUpper) {
            negateCharacterClass(chars)
          } else {
            RegexCharacterClass(negated = false, characters = chars)
          }
        case 'v' | 'V' =>
          // vertical whitespace
          // see https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html
          // under "Predefined character classes"
          val chars: ListBuffer[RegexCharacterClassComponent] = ListBuffer(
            RegexChar('\u000B'), RegexChar('\u0085'), RegexChar('\u2028'), RegexChar('\u2029')
          )
          chars ++= Seq('n', 'f', 'r').map(RegexEscaped)
          if (ch.isUpper) {
            negateCharacterClass(chars)
          } else {
            RegexCharacterClass(negated = false, characters = chars)
          }
        case 'R' =>
          // linebreak sequence
          // see https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html
          // under "Linebreak matcher"
          val l = RegexSequence(ListBuffer(RegexChar('\u000D'), RegexChar('\u000A')))
          val r = RegexCharacterClass(false, ListBuffer[RegexCharacterClassComponent](
            RegexChar('\u000A'), RegexChar('\u000B'), RegexChar('\u000C'), RegexChar('\u000D'),
            RegexChar('\u0085'), RegexChar('\u2028'), RegexChar('\u2029')
          ))
          RegexGroup(true, RegexChoice(l, r), None)
        case _ if escapeChars.contains(ch) =>
          RegexChar(escapeChars(ch))
        case _ if regexPunct.contains(ch) && !regexMetaChars.contains(ch) =>
          RegexChar(ch)
        case _ =>
          regex
      }

      case RegexCharacterRange(_, _) =>
        regex

      case RegexCharacterClass(negated, characters) =>
        characters.foreach {
          case r @ RegexChar(ch) if ch == '[' || ch == ']' =>
            // examples:
            // - "[a[]" should match the literal characters "a" and "["
            // - "[a-b[c-d]]" is supported by Java but not cuDF
            throw new RegexUnsupportedException(
              "Nested character classes are not supported", r.position)
          case _ =>
        }
        val components = ListBuffer(characters.toSeq
          .map {
            case r @ RegexChar(ch) if "^$.".contains(ch) => r
            case ch => rewrite(ch, replacement, None, flags) match {
              case valid: RegexCharacterClassComponent => valid
              case _ =>
                // this can happen when a character class contains a meta-sequence such as
                // `\s` that gets transpiled into another character class
                throw new RegexUnsupportedException("Character class contains one or more " +
                  "characters that cannot be transpiled to supported character-class components",
                  ch.position)
            }
          }: _*)

        if (negated) {
          negateCharacterClass(components)
        } else {
          RegexCharacterClass(negated, components)
        }

      case sequence @ RegexSequence(parts) =>
        if (parts.isEmpty) {
          // examples: "", "()", "a|", "|b"
          throw new RegexUnsupportedException("Empty sequence not supported",
            sequence.position)
        }
        if (isRegexChar(parts.head, '|')) {
          // example: "|b"
          throw new RegexUnsupportedException("Choice with one empty side not supported",
            parts.head.position)
        }
        if (isRegexChar(parts.last, '|')) {
          // example: "a|"
          throw new RegexUnsupportedException("Choice with one empty side not supported",
            parts.last.position)
        }
        if (isRegexChar(parts.head, '{')) {
          // example: "{"
          // cuDF would treat this as a quantifier even though in this
          // context (being at the start of a sequence) it is not quantifying anything
          // note that we could choose to escape this in the transpiler rather than
          // falling back to CPU
          throw new RegexUnsupportedException("Token preceding '{' is not quantifiable",
            parts.head.position)
        }
        if (parts.forall(isBeginOrEndLineAnchor)) {
          throw new RegexUnsupportedException(
            "Sequences that only contain '^' or '$' are not supported", sequence.position)
        }

        def popBackrefIfNecessary(capture: Boolean): Unit = {
          if (mode == RegexReplaceMode && !capture) {
            replacement match {
              case Some(repl) =>
                repl.popBackref()
              case _ =>
            }
          }
        }

        // Special handling for line anchor ($)
        // This code is implemented here because to make it work in cuDF, we have to reorder
        // the items in the regex.
        // In the JVM, regexes like "\n$" and "$\n" have similar treatment
        RegexSequence(parts.foldLeft((new ListBuffer[RegexAST](),
          Option.empty[RegexAST]))((m, part) => {
            val (r, last) = m
            last match {
              // when the previous character is a line anchor ($), the JVM has special handling
              // when matching against line terminator characters
              case Some(RegexChar('$')) | Some(RegexEscaped('Z')) =>
                part match {
                  case RegexGroup(capture, RegexSequence(
                      ListBuffer(RegexCharacterClass(true, parts))), _)
                      if parts.forall(!isBeginOrEndLineAnchor(_)) =>
                    popBackrefIfNecessary(capture)
                  case RegexGroup(capture, RegexCharacterClass(true, parts), _)
                      if parts.forall(!isBeginOrEndLineAnchor(_)) =>
                    popBackrefIfNecessary(capture)
                  case RegexCharacterClass(true, parts)
                      if parts.forall(!isBeginOrEndLineAnchor(_)) =>
                    popBackrefIfNecessary(false)
                  case RegexChar(ch) if lineTerminatorChars.contains(ch) =>
                    // what's really needed here is negative lookahead, but that is not
                    // supported by cuDF
                    // in this case: $\n would transpile to (?!\r)\n$
                    throw new RegexUnsupportedException(s"Regex sequence $$\\$ch is not supported",
                      part.position)
                  case RegexEscaped('z') =>
                    // since \z is not supported by cudf
                    // we need to transpile $\z to $(?![\r\n\u0085\u2028\u2029])
                    // however, cudf doesn't support negative look ahead
                    throw new RegexUnsupportedException("Regex sequence $\\z is not supported",
                      part.position)
                  case RegexEscaped(a) if "bBsSdDwWaAf".contains(a) =>
                    throw new RegexUnsupportedException(
                      s"Regex sequences with \\$a are not supported around end-of-line markers " +
                        "like $ or \\Z at position", part.position)
                  case _ =>
                    r.append(rewrite(part, replacement, last, flags))
                }
              case _ =>
                r.append(rewrite(part, replacement, last, flags))
            }
            r.last match {
              case RegexEmpty() =>
                (r, last)
              case _ =>
                (r, Some(part))
            }
        })._1)

      case RegexRepetition(base, quantifier) => (base, quantifier) match {
        case (_, SimpleQuantifier(ch)) if mode == RegexSplitMode
            && flags.emptyRepetition && "?*".contains(ch) =>
          // example: pattern " ?", input "] b[", replace with "X":
          // java: X]XXbX[X
          // cuDF: XXXX] b[
          // see https://github.com/NVIDIA/spark-rapids/issues/4884
          throw new RegexUnsupportedException(
            "regexp_split on GPU does not support empty match repetition consistently with Spark",
            quantifier.position)

        case (_, QuantifierVariableLength(0, _)) if mode == RegexSplitMode
            && flags.emptyRepetition =>
          // see https://github.com/NVIDIA/spark-rapids/issues/4884
          throw new RegexUnsupportedException(
            "regexp_split on GPU does not support empty match repetition consistently with Spark",
            quantifier.position)

        case (_, QuantifierVariableLength(0, Some(0))) if mode != RegexFindMode =>
          throw new RegexUnsupportedException(
            "regex_replace and regex_split on GPU do not support repetition with {0,0}",
            quantifier.position)

        case (_, QuantifierFixedLength(0)) if mode != RegexFindMode =>
          throw new RegexUnsupportedException(
            "regex_replace and regex_split on GPU do not support repetition with {0}",
            quantifier.position)

        case (RegexGroup(capture, term, _), SimpleQuantifier(ch))
            if "+*".contains(ch) && !isSupportedRepetitionBase(term) =>
          (term, ch) match {
            // \Z is not supported in groups
            case (RegexEscaped('A'), '+') |
                (RegexSequence(ListBuffer(RegexEscaped('A'))), '+') =>
              // (\A)+ can be transpiled to (\A) (dropping the repetition)
              // we use rewrite(...) here to handle logic regarding modes
              // (\A is not supported in RegexSplitMode)
              RegexGroup(capture, rewrite(term, replacement, previous, flags), None)
            // NOTE: (\A)* can be transpiled to (\A)?
            // however, (\A)? is not supported in libcudf yet
            case _ =>
              val unsupportedTerm = getUnsupportedRepetitionBase(term)
              throw new RegexUnsupportedException(
                s"cuDF does not support repetition of group containing: " +
                  s"${unsupportedTerm.toRegexString}", term.position)
          }
        case (RegexGroup(capture, term, _), QuantifierVariableLength(n, _))
            if !isSupportedRepetitionBase(term) =>
          term match {
            // \Z is not supported in groups
            case RegexEscaped('A') |
              RegexSequence(ListBuffer(RegexEscaped('A'))) if n > 0 =>
              // (\A){1,} can be transpiled to (\A) (dropping the repetition)
              // we use rewrite(...) here to handle logic regarding modes
              // (\A is not supported in RegexSplitMode)
              RegexGroup(capture, rewrite(term, replacement, previous, flags), None)
            // NOTE: (\A)* can be transpiled to (\A)?
            // however, (\A)? is not supported in libcudf yet
            case _ =>
              val unsupportedTerm = getUnsupportedRepetitionBase(term)
              throw new RegexUnsupportedException(
                s"cuDF does not support repetition of group containing: " +
                  s"${unsupportedTerm.toRegexString}", term.position)
          }
        case (RegexGroup(capture, term, _), QuantifierFixedLength(n))
            if !isSupportedRepetitionBase(term) =>
          term match {
            // \Z is not supported in groups
            case RegexEscaped('A') |
              RegexSequence(ListBuffer(RegexEscaped('A'))) if n > 0 =>
              // (\A){1,} can be transpiled to (\A) (dropping the repetition)
              // we use rewrite(...) here to handle logic regarding modes
              // (\A is not supported in RegexSplitMode)
              RegexGroup(capture, rewrite(term, replacement, previous, flags), None)
            // NOTE: (\A)* can be transpiled to (\A)?
            // however, (\A)? is not supported in libcudf yet
            case _ =>
              val unsupportedTerm = getUnsupportedRepetitionBase(term)
              throw new RegexUnsupportedException(
                s"cuDF does not support repetition of group containing: " +
                  s"${unsupportedTerm.toRegexString}", term.position)
          }
        case (RegexGroup(_, term, _), SimpleQuantifier(ch)) if ch == '?' =>
          if (isEntirelyWordBoundary(term) || isEntirelyLineAnchor(term)) {
            throw new RegexUnsupportedException(
                s"cuDF does not support repetition of: ${term.toRegexString}", term.position)
          }
          RegexRepetition(rewrite(base, replacement, None, flags), quantifier)
        case (RegexEscaped(ch), SimpleQuantifier('+')) if "AZ".contains(ch) =>
          // \A+ can be transpiled to \A (dropping the repetition)
          // \Z+ can be transpiled to \Z (dropping the repetition)
          // we use rewrite(...) here to handle logic regarding modes
          // (\A and \Z are not supported in RegexSplitMode)
          rewrite(base, replacement, previous, flags)
        // NOTE: \A* can be transpiled to \A?
        // however, \A? is not supported in libcudf yet
        case (RegexEscaped(ch), QuantifierFixedLength(n)) if n > 0 && "AZ".contains(ch) =>
          // \A{2} can be transpiled to \A (dropping the repetition)
          // \Z{2} can be transpiled to \Z (dropping the repetition)
          rewrite(base, replacement, previous, flags)
        case (RegexEscaped(ch), QuantifierVariableLength(n,_)) if n > 0 && "AZ".contains(ch) =>
          // \A{1,5} can be transpiled to \A (dropping the repetition)
          // \Z{1,} can be transpiled to \Z (dropping the repetition)
          rewrite(base, replacement, previous, flags)
        case _ if isSupportedRepetitionBase(base) =>
          RegexRepetition(rewrite(base, replacement, None, flags), quantifier)
        case (RegexRepetition(_, SimpleQuantifier('*')), SimpleQuantifier('+')) =>
          throw new RegexUnsupportedException("Possessive quantifier *+ not supported",
            quantifier.position)
        case (RegexRepetition(_, SimpleQuantifier('?' | '*' | '+')), SimpleQuantifier('?')) =>
          RegexRepetition(rewrite(base, replacement, None, flags), quantifier)
        case _ =>
          throw new RegexUnsupportedException("Preceding token cannot be quantified",
            quantifier.position)

      }

      case RegexChoice(l, r) =>
        val ll = rewrite(l, replacement, None, flags)
        val rr = rewrite(r, replacement, None, flags)

        // cuDF does not support zero-length repetition in replace or split mode
        // cuDF does support +, fixed-length, and variable length with min > 0
        if (mode != RegexFindMode) {
          if (isRepetition(ll, true)) {
            throw new RegexUnsupportedException(
              "cuDF does not support replace or split with zero-length repetition on one side of a"
              + " choice",
              l.position)
          } else if (isRepetition(rr, true)) {
            throw new RegexUnsupportedException(
              "cuDF does not support replace or split with zero-length repetition on one side of a"
              + " choice",
              r.position)
          }
        }

        if (mode == RegexSplitMode) {
          if (beginsWithLineAnchor(ll) || beginsWithLineAnchor(rr) ||
              endsWithLineAnchor(ll) || endsWithLineAnchor(rr)) {
            throw new RegexUnsupportedException(
              "cuDF does not support either side of a choice containing a line anchor in split "
               + "mode", l.position)
          }
        }

        // cuDF does not support terms ending with word boundaries on one side
        // of a choice, such as "\\b|a"
        if (endsWithWordBoundary(ll)) {
          throw new RegexUnsupportedException(
            "cuDF does not support terms ending with word boundaries on one side of a choice",
            l.position)
        } else if (endsWithWordBoundary(rr)) {
          throw new RegexUnsupportedException(
            "cuDF does not support terms ending with word boundaries on one side of a choice",
            r.position)
        }

        (ll, rr) match {
          // ll = lazyQuantifier inside a choice
          case (RegexSequence(ListBuffer(RegexRepetition(
          RegexRepetition(_, SimpleQuantifier('?')), SimpleQuantifier('?')))), _) |
               // rr = lazyQuantifier inside a choice
               (_, RegexSequence(ListBuffer(RegexRepetition(
               RegexRepetition(_, SimpleQuantifier('?')), SimpleQuantifier('?'))))) =>
            throw new RegexUnsupportedException(
              "cuDF does not support lazy quantifier inside choice", r.position)
          case (_, RegexChoice(RegexSequence(_), RegexSequence(ListBuffer(RegexRepetition(
          RegexEscaped('A'), SimpleQuantifier('?')), _)))) =>
            throw new RegexUnsupportedException("Invalid regex pattern at position", r.position)
          case _ =>
        }
        RegexChoice(ll, rr)

      case g @ RegexGroup(_, _, Some(lookahead)) =>
        val msg = lookahead match {
          case RegexPositiveLookahead =>
            "Positive lookahead groups are not supported"
          case RegexNegativeLookahead =>
            "Negative lookahead groups are not supported"
        }
        throw new RegexUnsupportedException(msg, g.position)

      case RegexGroup(capture, term, _) =>
        term match {
          case RegexSequence(parts) =>
            parts.foreach { part =>
              if (isBeginOrEndLineAnchor(part)) {
                throw new RegexUnsupportedException(
                  "Line and string anchors are not supported in capture groups", part.position)
              }
              part match {
                case RegexRepetition(base, quantifier) => (base, quantifier) match {
                  case (_, QuantifierVariableLength(0, Some(0))) =>
                    throw new RegexUnsupportedException(
                      "Repetition with {0,0} not supported in capture groups",
                      quantifier.position)

                  case (_, QuantifierFixedLength(0)) =>
                    throw new RegexUnsupportedException(
                      "Reptition with {0} not supported in capture groups",
                      quantifier.position)
                  case _ =>
                }
                case _ =>
              }
            }
            RegexGroup(capture, rewrite(term, replacement, None, flags), None)
          case _ =>
            RegexGroup(capture, rewrite(term, replacement, None, flags), None)
        }

      case other =>
        throw new RegexUnsupportedException(s"Unhandled expression in transpiler: $other",
          other.position)
    }
  }

  private def contains(regex: RegexAST, f: RegexAST => Boolean,
      recurseIntoCharacterClasses: Boolean = true): Boolean = {
    if (f(regex)) {
      true
    } else {
      regex match {
        case RegexSequence(parts) =>
          parts.exists(x => contains(x, f, recurseIntoCharacterClasses))
        case RegexGroup(_, term, _) => contains(term, f, recurseIntoCharacterClasses)
        case RegexChoice(l, r) =>
          contains(l, f, recurseIntoCharacterClasses) ||
            contains(r, f, recurseIntoCharacterClasses)
        case RegexRepetition(term, _) => contains(term, f, recurseIntoCharacterClasses)
        case RegexCharacterClass(_, chars) if recurseIntoCharacterClasses =>
          chars.exists(ch => contains(ch, f, recurseIntoCharacterClasses))
        case RegexCharacterClass(_, _) => false
        case leaf => f(leaf)
      }
    }
  }

  @scala.annotation.tailrec
  private def isEntirely(regex: RegexAST, f: RegexAST => Boolean): Boolean = {
    regex match {
      case RegexSequence(parts) if parts.nonEmpty =>
        parts.forall(f)
      case RegexGroup(_, term, _) =>
        isEntirely(term, f)
      case _ => f(regex)
    }
  }

  private def isEntirelyWordBoundary(regex: RegexAST): Boolean = {
    isEntirely(regex, {
      case RegexEscaped(ch) if "bB".contains(ch) => true
      case _ => false
    })
  }

  private def isEntirelyLineAnchor(regex: RegexAST): Boolean = {
    isEntirely(regex, {
      case RegexEscaped('A') => true
      case other => isBeginOrEndLineAnchor(other)
    })
  }

  @scala.annotation.tailrec
  private def beginsWith(regex: RegexAST, f: RegexAST => Boolean): Boolean = {
    regex match {
      case RegexSequence(parts) if parts.nonEmpty =>
        val j = parts.indexWhere {
            case RegexEmpty() => false
            case _ => true
        }
        beginsWith(parts(j), f)
      case RegexGroup(_, term, _) =>
        beginsWith(term, f)
      case _ => f(regex)
    }

  }

  @scala.annotation.tailrec
  private def endsWith(regex: RegexAST, f: RegexAST => Boolean): Boolean = {
    regex match {
      case RegexSequence(parts) if parts.nonEmpty =>
        val j = parts.lastIndexWhere {
            case RegexEmpty() => false
            case _ => true
        }
        endsWith(parts(j), f)
      case RegexGroup(_, term, _) =>
        endsWith(term, f)
      case _ => f(regex)
    }
  }

  private def endsWithLineAnchor(e: RegexAST): Boolean = {
    endsWith(e, {
      case RegexEscaped(ch) if "AZ".contains(ch) => true
      case other => isBeginOrEndLineAnchor(other)
    })
  }

  private def beginsWithLineAnchor(e: RegexAST): Boolean = {
    beginsWith(e, {
      case RegexEscaped(ch) if "AZ".contains(ch) => true
      case other => isBeginOrEndLineAnchor(other)
    })
  }

  private def endsWithWordBoundary(e: RegexAST): Boolean = {
    endsWith(e, {
      case RegexEscaped(a) if "bB".contains(a) => true
      case _ => false
    })
  }

  private def isBeginOrEndLineAnchor(regex: RegexAST): Boolean = regex match {
    case RegexSequence(parts) => parts.nonEmpty && parts.forall(isBeginOrEndLineAnchor)
    case RegexGroup(_, term, _) => isBeginOrEndLineAnchor(term)
    case RegexChoice(l, r) => isBeginOrEndLineAnchor(l) && isBeginOrEndLineAnchor(r)
    case RegexRepetition(term, _) => isBeginOrEndLineAnchor(term)
    case RegexChar(ch) => ch == '^' || ch == '$'
    case RegexEscaped(ch) if "zZ".contains(ch) => true // \z gets translated to $
    case RegexEscaped(ch) if 'A' == ch => true
    case _ => false
  }

  private def isRegexChar(expr: RegexAST, value: Char): Boolean = expr match {
    case RegexChar(ch) => ch == value
    case _ => false
  }
}

sealed trait RegexAST {
  def children(): Seq[RegexAST]
  def toRegexString: String
  var position: Option[Int] = None
}

sealed case class RegexEmpty() extends RegexAST {
  override def children(): Seq[RegexAST] = Seq.empty
  override def toRegexString: String = ""
}

sealed case class RegexSequence(parts: ListBuffer[RegexAST]) extends RegexAST {
  def this(parts: ListBuffer[RegexAST], position: Int) = {
    this(parts)
    this.position = Some(position)
  }
  override def children(): Seq[RegexAST] = parts.toSeq
  override def toRegexString: String = parts.map(_.toRegexString).mkString
}

sealed case class RegexGroup(capture: Boolean, term: RegexAST,
    lookahead: Option[RegexLookahead])
    extends RegexAST {
  def this(capture: Boolean, term: RegexAST) = {
    this(capture, term, None)
  }
  def this(capture: Boolean, term: RegexAST, position: Int) = {
    this(capture, term, None)
    this.position = Some(position)
  }
  def this(capture: Boolean, term: RegexAST, position: Int, lookahead: Option[RegexLookahead]) = {
    this(capture, term, lookahead)
    this.position = Some(position)
  }
  override def children(): Seq[RegexAST] = Seq(term)
  override def toRegexString: String = if (capture) {
    s"(${term.toRegexString})"
  } else if (lookahead.isDefined) {
    lookahead match {
      case Some(RegexPositiveLookahead) => s"(=${term.toRegexString})"
      case Some(RegexNegativeLookahead) => s"(!${term.toRegexString})"
      case _ => throw new IllegalStateException("Should not reach here")
    }
  } else {
    s"(?:${term.toRegexString})"
  }
}

sealed case class RegexChoice(a: RegexAST, b: RegexAST) extends RegexAST {
  def this(a: RegexAST, b: RegexAST, position: Int) = {
    this(a, b)
    this.position = Some(position)
  }
  override def children(): Seq[RegexAST] = Seq(a, b)
  override def toRegexString: String = s"${a.toRegexString}|${b.toRegexString}"
}

sealed case class RegexRepetition(a: RegexAST, quantifier: RegexQuantifier) extends RegexAST {
  def this(a: RegexAST, quantifier: RegexQuantifier, position: Int) = {
    this(a, quantifier)
    this.position = Some(position)
  }
  override def children(): Seq[RegexAST] = Seq(a)
  override def toRegexString: String = s"${a.toRegexString}${quantifier.toRegexString}"
}

sealed trait RegexQuantifier extends RegexAST

sealed case class SimpleQuantifier(ch: Char) extends RegexQuantifier {
  def this(ch: Char, position: Int) = {
    this(ch)
    this.position = Some(position)
  }
  override def children(): Seq[RegexAST] = Seq.empty
  override def toRegexString: String = ch.toString
}

sealed case class QuantifierFixedLength(length: Int)
    extends RegexQuantifier {
  def this(length: Int, position: Int) = {
    this(length)
    this.position = Some(position)
  }
  override def children(): Seq[RegexAST] = Seq.empty
  override def toRegexString: String = {
    s"{$length}"
  }
}

sealed case class QuantifierVariableLength(minLength: Int, maxLength: Option[Int])
    extends RegexQuantifier{
  def this(minLength: Int, maxLength: Option[Int], position: Int) = {
    this(minLength, maxLength)
    this.position = Some(position)
  }
  override def children(): Seq[RegexAST] = Seq.empty
  override def toRegexString: String = {
    maxLength match {
      case Some(max) =>
        s"{$minLength,$max}"
      case _ =>
        s"{$minLength,}"
    }
  }
}

sealed trait RegexCharacterClassComponent extends RegexAST

sealed case class RegexHexDigit(a: String) extends RegexCharacterClassComponent {
  def this(a: String, position: Int) = {
    this(a)
    this.position = Some(position)
  }
  val codePoint = Integer.parseInt(a, 16)

  override def children(): Seq[RegexAST] = Seq.empty
  override def toRegexString: String = {
    if (a.length == 2) {
      s"\\x$a"
    } else {
      s"\\x{$a}"
    }
  }
}

sealed case class RegexOctalChar(a: String) extends RegexCharacterClassComponent {
  def this(a: String, position: Int) = {
    this(a)
    this.position = Some(position)
  }
  val codePoint = Integer.parseInt(a, 8)

  override def children(): Seq[RegexAST] = Seq.empty
  override def toRegexString: String = s"\\$a"
}

sealed case class RegexChar(ch: Char) extends RegexCharacterClassComponent {
  def this(ch: Char, position: Int) = {
    this(ch)
    this.position = Some(position)
  }
  override def children(): Seq[RegexAST] = Seq.empty
  override def toRegexString: String = ch.toString
}

sealed case class RegexEscaped(a: Char) extends RegexCharacterClassComponent {
  def this(a: Char, position: Int) = {
    this(a)
    this.position = Some(position)
  }
  override def children(): Seq[RegexAST] = Seq.empty
  override def toRegexString: String = s"\\$a"
}

sealed case class RegexCharacterRange(start: RegexCharacterClassComponent,
    end: RegexCharacterClassComponent)
  extends RegexCharacterClassComponent{
  def this(start: RegexCharacterClassComponent,
           end: RegexCharacterClassComponent,
           position: Int) = {
    this(start, end)
    this.position = Some(position)
  }

  override def children(): Seq[RegexAST] = Seq.empty
  override def toRegexString: String =  s"${start.toRegexString}-${end.toRegexString}"
}

sealed case class RegexCharacterClass(
    var negated: Boolean,
    var characters: ListBuffer[RegexCharacterClassComponent])
  extends RegexAST {
  def this(
      negated: Boolean,
      characters: ListBuffer[RegexCharacterClassComponent],
      position: Int) = {
    this(negated, characters)
    this.position = Some(position)
  }

  override def children(): Seq[RegexAST] = characters.toSeq

  def append(ch: Char): Unit = {
    characters += RegexChar(ch)
  }

  def append(component: RegexCharacterClassComponent): Unit = {
    characters += component
  }

  def appendEscaped(ch: Char): Unit = {
    characters += RegexEscaped(ch)
  }

  def appendRange(start: RegexCharacterClassComponent,
      end: RegexCharacterClassComponent): Unit = {
    characters += RegexCharacterRange(start, end)
  }

  override def toRegexString: String = {
    val builder = new StringBuilder("[")
    if (negated) {
      builder.append("^")
    }
    for (a <- characters) {
      a match {
        case RegexChar(ch) if requiresEscaping(ch) =>
          // cuDF has stricter escaping requirements for certain characters
          // within a character class compared to Java or Python regex
          builder.append(s"\\$ch")
        case other =>
          builder.append(other.toRegexString)
      }
    }
    builder.append("]")
    builder.toString()
  }

  private def requiresEscaping(ch: Char): Boolean = {
    // there are likely other cases that we will need to add here but this
    // covers everything we have seen so far during fuzzing
    ch match {
      case '-' =>
        // cuDF requires '-' to be escaped when used as a character within a character
        // to disambiguate from the character range syntax 'a-b'
        true
      case _ =>
        false
    }
  }
}

sealed case class RegexBackref(num: Int, isNew: Boolean = false) extends RegexAST {
  def this(num: Int, isNew: Boolean, position: Int) = {
    this(num, isNew)
    this.position = Some(position)
  }
  override def children(): Seq[RegexAST] = Seq.empty
  // Backrefs marked as internally generated are emitted in braced `${N}` form so
  // `backrefConversion` can tell them apart from user-authored `$N` tokens and pass them
  // through verbatim. User-authored backrefs keep the raw `$N` form so the
  // greedy-with-backoff parser in `backrefConversion` honors the user's pattern group count.
  override def toRegexString: String = if (isNew) s"$${$num}" else s"$$$num"
}

sealed case class RegexReplacement(parts: ListBuffer[RegexAST],
    var numCaptureGroups: Int = 0) extends RegexAST {
  def this(parts: ListBuffer[RegexAST], numCaptureGroups: Int, position: Int) = {
    this(parts, numCaptureGroups)
    this.position = Some(position)
  }
  override def children(): Seq[RegexAST] = parts.toSeq
  override def toRegexString: String = parts.map(_.toRegexString).mkString

  def appendBackref(num: Int): Unit = {
    numCaptureGroups += 1
    parts += RegexBackref(num, true)
  }

  def popBackref(): Unit = {
    parts.last match {
      case RegexBackref(_, true) => {
        numCaptureGroups -= 1
        parts.trimEnd(1)
      }
      case _ =>
    }
  }

  def hasBackrefs: Boolean = numCaptureGroups > 0
}

class RegexUnsupportedException(message: String, index: Option[Int])
  extends SQLException {
  override def getMessage: String = {
    index match {
      case Some(i) => s"$message near index $i"
      case _ => message
    }
  }
}

sealed trait RegexOptimizationType
object RegexOptimizationType {
  case class StartsWith(literal: String) extends RegexOptimizationType
  case class Contains(literal: String) extends RegexOptimizationType
  case class PrefixRange(literal: String, length: Int, rangeStart: Int, rangeEnd: Int) 
    extends RegexOptimizationType
  case class MultipleContains(literals: Seq[UTF8String]) extends RegexOptimizationType
  case object NoOptimization extends RegexOptimizationType
}

object RegexRewrite {

  @scala.annotation.tailrec
  private def removeBrackets(astLs: collection.Seq[RegexAST]): collection.Seq[RegexAST] = {
    astLs match {
      case collection.Seq(RegexGroup(_, RegexSequence(terms), None)) => removeBrackets(terms)
      case _ => astLs
    }
  }

  /* 
   * Extracts the prefix range pattern info from the given AST sequence.
   * 
   * @param astLs The AST sequence to extract the prefix range pattern from.
   * @return Some(prefix, length, start, end) if astLs is a `prefix[start-end]{x}` pattern
   * None otherwise. start and end are the code points of the start and end characters.
   */
  private def getPrefixRangePattern(astLs: collection.Seq[RegexAST]): 
      Option[(String, Int, Int, Int)] = {
    val haveLiteralPrefix = isLiteralString(astLs.dropRight(1))
    val endsWithRange = astLs.lastOption match {
      case Some(ast) => removeBrackets(collection.Seq(ast)) match {
        case collection.Seq(RegexRepetition(
            RegexCharacterClass(false, ListBuffer(RegexCharacterRange(a,b))), 
            quantifier)) => {
          val (start, end) = (a, b) match {
            case (RegexChar(start), RegexChar(end)) => (start, end)
            case _ => return None
          }
          val length = quantifier match {
            // In Rlike, contains [a-b]{minLen,maxLen} pattern is equivalent to contains 
            // [a-b]{minLen} because the matching will return the result once it finds the 
            // minimum match so y here is unnecessary.
            case QuantifierVariableLength(minLen, _) => minLen
            case QuantifierFixedLength(len) => len
            case SimpleQuantifier(ch) => ch match {
              case '*' | '?' => 0
              case '+' => 1
              case _ => return None
            }
            case _ => return None
          }
          // Convert start and end to code points
          Some((length, start.toInt, end.toInt))
        }
        case _ => None
      }
      case _ => None
    }
    (haveLiteralPrefix, endsWithRange) match {
      case (true, Some((length, start, end))) => {
        val prefix = RegexCharsToString(astLs.dropRight(1))
        Some((prefix, length, start, end))
      }
      case _ => None
    }
  }

  private def isLiteralString(astLs: collection.Seq[RegexAST]): Boolean = {
    removeBrackets(astLs).forall {
      case RegexChar(ch) => !regexMetaChars.contains(ch)
      case _ => false
    }
  }

  private def getMultipleContainsLiterals(ast: RegexAST): Seq[UTF8String] = {
    ast match {
      case RegexGroup(_, term, _) => getMultipleContainsLiterals(term)
      case RegexChoice(RegexSequence(parts), ls) if isLiteralString(parts) => {
        getMultipleContainsLiterals(ls) match {
          case Seq() => Seq.empty
          case literals => UTF8String.fromString(RegexCharsToString(parts)) +: literals
        }
      }
      case RegexSequence(parts) if (isLiteralString(parts)) => 
          Seq(UTF8String.fromString(RegexCharsToString(parts)))
      case _ => Seq.empty
    }
  }

  private def isWildcard(ast: RegexAST): Boolean = {
    ast match {
      case RegexRepetition(RegexChar('.'), SimpleQuantifier('*')) => true
      case RegexSequence(parts) if parts.forall(isWildcard) => true
      case RegexGroup(_, term, _) if isWildcard(term) => true
      case _ => false
    }
  }

  private def stripLeadingWildcards(astLs: collection.Seq[RegexAST]): 
      collection.Seq[RegexAST] = {
    astLs.dropWhile(isWildcard)
  }

  private def stripTailingWildcards(astLs: collection.Seq[RegexAST]): 
      collection.Seq[RegexAST] = {
    astLs.reverse.dropWhile(isWildcard).reverse
  }

  private def RegexCharsToString(chars: collection.Seq[RegexAST]): String = {
    removeBrackets(chars).map {
      case RegexChar(ch) => ch
      case _ => throw new IllegalArgumentException("Invalid character")
    }.mkString
  }

  /**
   * Matches the given regex ast to a regex optimization type for regex rewrite
   * optimization.
   *
   * @param ast Abstract Syntax Tree parsed from a regex pattern.
   * @return The `RegexOptimizationType` for the given pattern.
   */
  def matchSimplePattern(ast: RegexAST): RegexOptimizationType = {
    val astLs = ast match {
      case RegexSequence(_) => ast.children()
      case _ => Seq(ast)
    }
    val noTailingWildcards = stripTailingWildcards(astLs)
    if (noTailingWildcards.headOption.exists(
        ast => ast == RegexChar('^') || ast == RegexEscaped('A'))) {
      val possibleLiteral = noTailingWildcards.drop(1)
      if (isLiteralString(possibleLiteral)) {
        return RegexOptimizationType.StartsWith(RegexCharsToString(possibleLiteral))
      }
    }

    val noStartsWithAst = removeBrackets(stripLeadingWildcards(noTailingWildcards))

    // Check if the pattern is a contains literal pattern
    if (isLiteralString(noStartsWithAst)) {
      // literal or .*(literal).* => contains literal
      return RegexOptimizationType.Contains(RegexCharsToString(noStartsWithAst))
    }

    // Check if the pattern is a multiple contains literal pattern (e.g. "abc|def|ghi")
    if (noStartsWithAst.length == 1) {
      val containsLiterals = getMultipleContainsLiterals(noStartsWithAst.head)
      if (!containsLiterals.isEmpty) {
        return RegexOptimizationType.MultipleContains(containsLiterals)
      }
    }

    // Check if the pattern is a prefix range pattern (e.g. "abc[a-z]{3}")
    val prefixRangeInfo = getPrefixRangePattern(noStartsWithAst)
    if (prefixRangeInfo.isDefined) {
      val (prefix, length, start, end) = prefixRangeInfo.get
      // (literal[a-b]{x,y}) => prefix range pattern
      return RegexOptimizationType.PrefixRange(prefix, length, start, end)
    }
    
    // return NoOptimization if the pattern is not a simple pattern and use cuDF
    RegexOptimizationType.NoOptimization
  }
}
