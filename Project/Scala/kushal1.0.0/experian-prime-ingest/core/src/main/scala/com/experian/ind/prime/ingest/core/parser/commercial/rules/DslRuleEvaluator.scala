package com.experian.ind.prime.ingest.core.parser.commercial.rules

import com.experian.ind.prime.ingest.core.Util.parser.commercial.DualLogger
import scala.util.{Try, Success, Failure}

/**
 * DslRuleEvaluator - Evaluates rule conditions using a simple Domain-Specific Language
 * Supports readable conditions without SQL complexity
 * 
 * Supported Operators:
 *   Logical: AND, OR, NOT
 *   Comparison: >, <, >=, <=, ==, !=
 *   Null checks: IS NULL, IS NOT NULL
 *   String functions: LENGTH(field.value), TRIM(field.value)
 * 
 * Example DSL Conditions:
 *   "field.is_mandatory AND field.value_length > field.max_length"
 *   "record.bs_count == 1 AND record.rs_count >= 1"
 *   "field.value IS NOT NULL AND TRIM(field.value) != ''"
 * 
 * Evaluation:
 *   val context = RuleContext.forFieldValidation(value="ABC", isMandatory=true, maxLength=10)
 *   val result = evaluator.evaluate("field.is_mandatory AND field.value_length <= field.max_length", context)
 */
class DslRuleEvaluator {
  private val logger = DualLogger(getClass)
  
  // OPTIMIZATION: Pre-compiled operator list for comparisons
  private val comparisonOps = List(">=", "<=", "==", "!=", ">", "<", "=")
  private val upperCaseTrue = "TRUE"
  private val upperCaseFalse = "FALSE"

  /**
   * Evaluate a DSL condition against a RuleContext
   * Returns true if condition passes, false if it fails
   */
  def evaluate(dslCondition: String, context: RuleContext): Boolean = {
    Try {
      if (dslCondition == null || dslCondition.trim.isEmpty) {
        logger.debug("[DSL] DSL condition is empty; defaulting to PASS")
        return true
      }

      val result = parseAndEvaluate(dslCondition.trim, context)
      logger.debug(s"[DSL] Evaluated: '$dslCondition' => $result")
      result
    } match {
      case Success(value) => value
      case Failure(ex) =>
        logger.error(s"[DSL] Failed to evaluate condition: '$dslCondition'", ex)
        false  // Fail-safe: reject on evaluation error
    }
  }

  /**
   * Parse and evaluate DSL condition recursively
   * Handles logical operators (OR, AND, NOT) and comparisons
   * OPTIMIZATION: Early operator detection, caching, short-circuit evaluation
   */
  private def parseAndEvaluate(condition: String, context: RuleContext): Boolean = {
    val trimmed = condition.trim
    if (trimmed.isEmpty) return false

    // OPTIMIZATION: Remove outer parentheses only once
    val cleaned = if (trimmed.startsWith("(") && trimmed.endsWith(")") && trimmed.length >= 2) {
      trimmed.substring(1, trimmed.length - 1).trim
    } else {
      trimmed
    }

    // OPTIMIZATION: Check for OR operator with early exit
    val orIdx = findOperatorIndex(cleaned, " OR ")
    if (orIdx >= 0) {
      val leftPart = cleaned.substring(0, orIdx).trim
      val rightPart = cleaned.substring(orIdx + 4).trim  // " OR " has 4 characters
      // Short-circuit evaluation: return on first true
      return parseAndEvaluate(leftPart, context) || parseAndEvaluate(rightPart, context)
    }

    // OPTIMIZATION: Check for AND operator with early exit
    val andIdx = findOperatorIndex(cleaned, " AND ")
    if (andIdx >= 0) {
      val leftPart = cleaned.substring(0, andIdx).trim
      val rightPart = cleaned.substring(andIdx + 5).trim
      // Short-circuit evaluation: return on first false
      return parseAndEvaluate(leftPart, context) && parseAndEvaluate(rightPart, context)
    }

    // Handle NOT operator
    val cleanedLower = cleaned.toLowerCase
    if (cleanedLower.startsWith("not ")) {
      val remaining = cleaned.substring(4).trim
      return !parseAndEvaluate(remaining, context)
    }

    // Handle comparison operators
    evaluateComparison(cleaned, context)
  }

  /**
   * OPTIMIZATION: Find operator index efficiently
   */
  private def findOperatorIndex(text: String, op: String): Int = {
    var parenDepth = 0
    var i = 0
    val opLen = op.length
    val textLen = text.length
    while (i <= textLen - opLen) {
      if (text.charAt(i) == '(') parenDepth += 1
      else if (text.charAt(i) == ')') parenDepth -= 1
      else if (parenDepth == 0 && text.regionMatches(true, i, op, 0, opLen)) {
        return i
      }
      i += 1
    }
    -1
  }

  /**
   * Evaluate comparison expressions
   * Examples: field.is_mandatory, field.value_length > 10, record.bs_count == 1
   * OPTIMIZATION: Efficient operator detection, conditional logging
   */
  private def evaluateComparison(expr: String, context: RuleContext): Boolean = {
    val trimmed = expr.trim

    // IS NOT NULL check - OPTIMIZATION: Use indexOf instead of containsOperator
    val isNotNullIdx = trimmed.lastIndexOf(" IS NOT NULL")
    if (isNotNullIdx > 0 && isNotNullIdx == trimmed.length - 12) {
      val field = trimmed.substring(0, isNotNullIdx).trim
      val value = resolveValue(field, context)
      val result = value.isDefined && value.get != null
      logger.debug(s"[DSL] IS NOT NULL: $field => $result")
      return result
    }

    // IS NULL check
    val isNullIdx = trimmed.lastIndexOf(" IS NULL")
    if (isNullIdx > 0 && isNullIdx == trimmed.length - 8) {
      val field = trimmed.substring(0, isNullIdx).trim
      val value = resolveValue(field, context)
      val result = value.isEmpty || value.get == null
      logger.debug(s"[DSL] IS NULL: $field => $result")
      return result
    }

    // OPTIMIZATION: Check comparison operators efficiently without full split
    for (op <- comparisonOps) {
      val opWithSpaces = s" $op "
      val opIdx = trimmed.indexOf(opWithSpaces)
      if (opIdx > 0) {
        val leftPart = trimmed.substring(0, opIdx).trim
        val rightPart = trimmed.substring(opIdx + opWithSpaces.length).trim
        val left = resolveValue(leftPart, context)
        val right = resolveValue(rightPart, context)
        // Treat both = and == as equality comparison
        val comparisonOp = if (op == "=") "==" else op
        val result = compare(left, right, comparisonOp)
        logger.debug(s"[DSL] Comparison: $leftPart $op $rightPart => $result")
        return result
      }
    }

    // Simple field reference (treat as boolean)
    val value = resolveValue(trimmed, context)
    val result = isTruthy(value)
    logger.debug(s"[DSL] Boolean check: $trimmed => $result (value=$value)")
    result
  }

  /**
   * Resolve a value from context
   * Handles:
   *   - Field references: field.value, record.bs_count
   *   - Function calls: LENGTH(field.value), TRIM(field.value)
   *   - Literals: 10, "ABC", true
   * OPTIMIZATION: Cache uppercase for function detection, avoid double case conversion
   */
  private def resolveValue(expr: String, context: RuleContext): Option[Any] = {
    val trimmed = expr.trim
    if (trimmed.isEmpty) return None

    // OPTIMIZATION: Cache uppercase conversion
    val upperTrimmed = trimmed.toUpperCase
    
    // Function call: LENGTH(field.value)
    if (upperTrimmed.startsWith("LENGTH(") && trimmed.endsWith(")")) {
      val arg = trimmed.substring(7, trimmed.length - 1).trim
      return resolveValue(arg, context).map { value =>
        value match {
          case s: String => s.length
          case _ => 0
        }
      }
    }

    // Function call: TRIM(field.value)
    if (upperTrimmed.startsWith("TRIM(") && trimmed.endsWith(")")) {
      val arg = trimmed.substring(5, trimmed.length - 1).trim
      return resolveValue(arg, context).map { value =>
        value match {
          case s: String => s.trim
          case _ => value
        }
      }
    }

    // OPTIMIZATION: Fast numeric literal check without Try
    if (isNumericLiteral(trimmed)) {
      return parseNumeric(trimmed)
    }

    // OPTIMIZATION: Cache boolean checks
    if (upperTrimmed == upperCaseTrue) return Some(true)
    if (upperTrimmed == upperCaseFalse) return Some(false)

    // String literal (quoted)
    if ((trimmed.startsWith("\"") && trimmed.endsWith("\"")) ||
        (trimmed.startsWith("'") && trimmed.endsWith("'"))) {
      return Some(trimmed.substring(1, trimmed.length - 1))
    }

    // === SEGMENT SEQUENCE FUNCTIONS FOR GENERIC RECORD-LEVEL VALIDATION ===
    // DSL: segment_sequence.containsAll(expected_segment_order)
    // Validates that all expected segments are present in the actual sequence
    if (trimmed == "segment_sequence.containsAll(expected_segment_order)") {
      val actual = context.getSegmentSequence
      val expected = context.getExpectedSegmentOrder
      if (expected.isEmpty) return Some(true)  // No constraints = pass
      val result = expected.forall(actual.contains)
      logger.debug(s"[DSL] segment_sequence.containsAll(expected_segment_order): actual=$actual, expected=$expected => $result")
      return Some(result)
    }

    // DSL: segment_sequence.inOrder(expected_segment_order)
    // Validates that all expected segments appear in the correct order
    if (trimmed == "segment_sequence.inOrder(expected_segment_order)") {
      val actual = context.getSegmentSequence
      val expected = context.getExpectedSegmentOrder
      if (expected.isEmpty) return Some(true)  // No constraints = pass
      // Walk actual, looking for expected in order
      var i = 0
      var j = 0
      while (i < actual.length && j < expected.length) {
        if (actual(i) == expected(j)) {
          j += 1
        }
        i += 1
      }
      val result = (j == expected.length)
      logger.debug(s"[DSL] segment_sequence.inOrder(expected_segment_order): actual=$actual, expected=$expected => $result")
      return Some(result)
    }

    // Field reference: field.value, record.bs_count, segment.tag
    val result = context.getValue(trimmed)
    if (result.isEmpty && (trimmed.contains("segment.") || trimmed.contains("field.") || trimmed.contains("record."))) {
      logger.debug(s"[DSL] Failed to resolve: '$trimmed' from context. Available keys: field=${context.getFieldData.keys.mkString(",")}, record=${context.getRecordData.keys.mkString(",")}, segment=${context.getSegmentData.keys.mkString(",")}")
    }
    result
  }

  /**
   * OPTIMIZATION: Check numeric literal without Try/Catch overhead
   */
  private def isNumericLiteral(s: String): Boolean = {
    if (s.isEmpty) return false
    var idx = 0
    if (s.charAt(0) == '-' || s.charAt(0) == '+') idx = 1
    if (idx >= s.length) return false
    var hasDot = false
    while (idx < s.length) {
      val c = s.charAt(idx)
      if (c == '.') {
        if (hasDot) return false
        hasDot = true
      } else if (!c.isDigit) {
        return false
      }
      idx += 1
    }
    true
  }

  /**
   * OPTIMIZATION: Parse numeric efficiently
   */
  private def parseNumeric(s: String): Option[Double] = {
    try {
      Some(if (s.contains(".")) s.toDouble else s.toInt.toDouble)
    } catch {
      case _: Exception => None
    }
  }

  /**
   * Compare two values based on operator
   */
  private def compare(left: Option[Any], right: Option[Any], op: String): Boolean = {
    (left, right) match {
      case (Some(l), Some(r)) => compareValues(l, r, op)
      case (None, Some(r)) => op match {
        case "!=" => r != null
        case "==" => r == null
        case _ => false
      }
      case (Some(l), None) => op match {
        case "!=" => l != null
        case "==" => l == null
        case _ => false
      }
      case (None, None) => op match {
        case "==" => true
        case "!=" => false
        case _ => false
      }
    }
  }

  /**
   * Compare two non-null values
   */
  private def compareValues(left: Any, right: Any, op: String): Boolean = {
    def numericCompare(l: Double, r: Double): Boolean = op match {
      case ">" => l > r
      case "<" => l < r
      case ">=" => l >= r
      case "<=" => l <= r
      case "==" => l == r
      case "!=" => l != r
      case _ => false
    }

    def stringCompare(l: String, r: String): Boolean = op match {
      case "==" => l == r
      case "!=" => l != r
      case ">" => l.compareTo(r) > 0
      case "<" => l.compareTo(r) < 0
      case ">=" => l.compareTo(r) >= 0
      case "<=" => l.compareTo(r) <= 0
      case _ => false
    }

    // Try numeric comparison
    val leftNum = toNumeric(left)
    val rightNum = toNumeric(right)

    if (leftNum.isDefined && rightNum.isDefined) {
      return numericCompare(leftNum.get, rightNum.get)
    }

    // String comparison
    val leftStr = left.toString
    val rightStr = right.toString
    stringCompare(leftStr, rightStr)
  }

  /**
   * Convert value to numeric if possible
   * OPTIMIZATION: Simplified logic, avoid unnecessary Try/Catch
   */
  private def toNumeric(value: Any): Option[Double] = {
    value match {
      case n: Number => Some(n.doubleValue())
      case s: String => 
        try {
          Some(s.toDouble)
        } catch {
          case _: Exception => None
        }
      case b: Boolean => Some(if (b) 1.0 else 0.0)
      case _ => None
    }
  }

  /**
   * Check if value is truthy
   * OPTIMIZATION: Direct pattern matching instead of map
   */
  private def isTruthy(value: Option[Any]): Boolean = {
    value match {
      case Some(b: Boolean) => b
      case Some(i: Int) => i != 0
      case Some(l: Long) => l != 0
      case Some(d: Double) => d != 0.0
      case Some(s: String) => s.nonEmpty && s.toUpperCase != upperCaseFalse
      case Some(null) => false
      case Some(_) => true
      case None => false
    }
  }

  /**
   * DEPRECATED: Use findOperatorIndex instead for better performance
   * Kept for backward compatibility if needed
   */
  @deprecated("Use findOperatorIndex instead", "2025-12-24")
  private def containsOperator(text: String, op: String): Boolean = {
    findOperatorIndex(text, op) >= 0
  }

  /**
   * DEPRECATED: Use splitByOperatorIndex instead for better performance
   * Kept for backward compatibility if needed
   */
  @deprecated("Use findOperatorIndex/splitByOperatorIndex instead", "2025-12-24")
  private def splitByOperator(text: String, op: String): Seq[String] = {
    val opIdx = findOperatorIndex(text, op)
    if (opIdx < 0) Seq(text) else {
      Seq(text.substring(0, opIdx).trim, text.substring(opIdx + op.length).trim)
    }
  }
}

/**
 * DSL Condition Builder - Helper for constructing readable DSL conditions
 * Provides predefined conditions and builder methods
 */
object DslConditions {

  /**
   * Field-level validation conditions
   */
  object Field {
    val isMandatory: String = "field.is_mandatory"

    val hasValue: String = "field.value IS NOT NULL AND TRIM(field.value) != ''"

    val isNullOrEmpty: String = "field.value IS NULL OR TRIM(field.value) == ''"

    def lengthExceeds(maxLengthField: String = "field.max_length"): String =
      s"LENGTH(field.value) > $maxLengthField"

    def lengthWithinLimit(maxLengthField: String = "field.max_length"): String =
      s"LENGTH(field.value) <= $maxLengthField"

    def mandatoryAndTooLong(maxLengthField: String = "field.max_length"): String =
      s"field.is_mandatory AND field.value IS NOT NULL AND TRIM(field.value) != '' AND LENGTH(field.value) > $maxLengthField"

    def mandatoryButMissing: String =
      "field.is_mandatory AND (field.value IS NULL OR TRIM(field.value) == '')"
  }

  /**
   * Record-level validation conditions
   */
  object Record {
    val hasValidSegmentStructure: String =
      "record.bs_count == 1 AND record.rs_count >= 1 AND record.cr_count >= 1"

    val noOutOfOrderSegments: String =
      "record.out_of_order_count == 0"

    val startsWithBS: String =
      "record.bs_count >= 1"

    def atLeastXSegments(count: Int): String =
      s"record.segment_count >= $count"

    def exactlyXSegments(count: Int): String =
      s"record.segment_count == $count"

    def crCountRange(min: Int, max: Int): String =
      s"record.cr_count >= $min AND record.cr_count <= $max"

    def gsCountOptional: String =
      "record.gs_count >= 0"
  }

  /**
   * Combine conditions with AND
   */
  def and(conditions: String*): String =
    conditions.filterNot(_.isEmpty).mkString(" AND ")

  /**
   * Combine conditions with OR
   */
  def or(conditions: String*): String =
    conditions.filterNot(_.isEmpty).mkString(" OR ")

  /**
   * Negate a condition
   */
  def not(condition: String): String =
    s"NOT ($condition)"
}
