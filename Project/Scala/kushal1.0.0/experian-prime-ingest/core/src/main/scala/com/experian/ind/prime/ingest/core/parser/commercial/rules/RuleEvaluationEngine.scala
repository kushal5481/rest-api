package com.experian.ind.prime.ingest.core.parser.commercial.rules

import com.experian.ind.prime.ingest.core.Util.parser.commercial.DualLogger
import com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial.RuleDefinition
import org.apache.spark.sql.SparkSession

/**
 * RuleEvaluationEngine - Orchestrates rule evaluation with fallback strategy
 * 
 * Evaluation Strategy:
 *   1. If rule has DSL_condition defined → Use DslRuleEvaluator (fast, simple)
 *   2. Else if rule has SQL_expression defined → Use SqlRuleEvaluator (powerful, complex)
 *   3. Else → Default to PASS (safe default)
 * 
 * This separation of concerns allows:
 *   - DSL for simple field/record validations
 *   - SQL for complex aggregations and cross-record logic
 *   - Smooth migration from SQL-only to DSL-first approach
 * 
 * Usage:
 *   val engine = new RuleEvaluationEngine(spark)
 *   val context = RuleContext.forFieldValidation(value="ABC", isMandatory=true, maxLength=10)
 *   val passed = engine.evaluateRule(rule, context)
 */
class RuleEvaluationEngine(spark: SparkSession) {
  private val logger = DualLogger(getClass)
  private val dslEvaluator = new DslRuleEvaluator()
  private val sqlEvaluator = new SqlRuleEvaluator(spark)

  /**
   * Evaluate a rule using the fallback strategy: DSL → SQL → PASS
   * 
   * @param rule The rule definition containing optional DSL_condition and SQL_expression
   * @param context The rule context containing field, record, and segment data
   * @return true if rule passes, false if it fails
   */
  def evaluateRule(rule: RuleDefinition, context: RuleContext): Boolean = {
    val ruleId = rule.rule_id
    val ruleName = rule.rule_name.getOrElse("UNKNOWN")

    try {
      // Strategy 1: Try DSL evaluation first (if DSL_condition is defined)
      if (hasDslCondition(rule)) {
        logger.info(s"[RULE][$ruleId] Using DSL evaluation")
        val dslCondition = rule.DSL_condition.map(_.trim).getOrElse("")
        val result = dslEvaluator.evaluate(dslCondition, context)
        logger.info(s"[RULE][$ruleId] DSL result: $result")
        return result
      }

      // Strategy 2: Fallback to SQL evaluation (if SQL_expression is defined)
      if (hasSqlExpression(rule)) {
        logger.info(s"[RULE][$ruleId] Using SQL evaluation (DSL not available)")
        val sqlExpression = rule.SQL_expression.map(_.trim).getOrElse("")
        val result = sqlEvaluator.evaluateOnRaw("df_field_value", rule)
        logger.info(s"[RULE][$ruleId] SQL result: $result")
        return result
      }

      // Default: Neither DSL nor SQL defined
      logger.warn(s"[RULE][$ruleId] No DSL_condition or SQL_expression defined; defaulting to PASS")
      true

    } catch {
      case ex: Exception =>
        logger.error(s"[RULE][$ruleId] Rule evaluation failed: ${ex.getMessage}", ex)
        false  // Fail-safe: reject on any evaluation error
    }
  }

  /**
   * Evaluate a record-level rule
   * Typically uses SQL for complex segment structure validations
   */
  def evaluateRecordRule(rule: RuleDefinition, context: RuleContext, rawRecordView: String = "df_raw_record"): Boolean = {
    val ruleId = rule.rule_id

    try {
      // For record-level rules, try DSL first, then SQL
      if (hasDslCondition(rule)) {
        logger.info(s"[RULE][$ruleId] Evaluating record-level rule using DSL")
        return dslEvaluator.evaluate(rule.DSL_condition.map(_.trim).getOrElse(""), context)
      }

      if (hasSqlExpression(rule)) {
        logger.info(s"[RULE][$ruleId] Evaluating record-level rule using SQL")
        return sqlEvaluator.evaluateOnRaw(rawRecordView, rule)
      }

      logger.warn(s"[RULE][$ruleId] No rule condition defined; defaulting to PASS")
      true
    } catch {
      case ex: Exception =>
        logger.error(s"[RULE][$ruleId] Record rule evaluation failed: ${ex.getMessage}", ex)
        false
    }
  }

  /**
   * Evaluate a field-level rule
   * Can use either DSL (simpler) or SQL (more powerful)
   */
  def evaluateFieldRule(rule: RuleDefinition, context: RuleContext, fieldValueView: String = "df_field_value"): Boolean = {
    val ruleId = rule.rule_id

    try {
      // Try DSL first (simpler, faster for field-level)
      if (hasDslCondition(rule)) {
        logger.info(s"[RULE][$ruleId] Evaluating field-level rule using DSL")
        return dslEvaluator.evaluate(rule.DSL_condition.map(_.trim).getOrElse(""), context)
      }

      // Fallback to SQL
      if (hasSqlExpression(rule)) {
        logger.info(s"[RULE][$ruleId] Evaluating field-level rule using SQL")
        return sqlEvaluator.evaluateOnRaw(fieldValueView, rule)
      }

      logger.warn(s"[RULE][$ruleId] No rule condition defined; defaulting to PASS")
      true
    } catch {
      case ex: Exception =>
        logger.error(s"[RULE][$ruleId] Field rule evaluation failed: ${ex.getMessage}", ex)
        false
    }
  }

  /**
   * Evaluate a segment-level rule
   * Segment-level rules validate segment structure (e.g., delimiter count)
   * Can use either DSL (simpler) or SQL (more powerful)
   */
  def evaluateSegmentRule(rule: RuleDefinition, context: RuleContext, segmentView: String = "df_segment"): Boolean = {
    val ruleId = rule.rule_id

    try {
      // Try DSL first (simpler, faster for segment-level)
      if (hasDslCondition(rule)) {
        logger.info(s"[RULE][$ruleId] Evaluating segment-level rule using DSL")
        return dslEvaluator.evaluate(rule.DSL_condition.map(_.trim).getOrElse(""), context)
      }

      // Fallback to SQL
      if (hasSqlExpression(rule)) {
        logger.info(s"[RULE][$ruleId] Evaluating segment-level rule using SQL")
        return sqlEvaluator.evaluateOnRaw(segmentView, rule)
      }

      logger.warn(s"[RULE][$ruleId] No rule condition defined; defaulting to PASS")
      true
    } catch {
      case ex: Exception =>
        logger.error(s"[RULE][$ruleId] Segment rule evaluation failed: ${ex.getMessage}", ex)
        false
    }
  }

  /**
   * Check if rule has a valid DSL condition defined
   */
  private def hasDslCondition(rule: RuleDefinition): Boolean = {

    rule.DSL_condition
      .map(_.trim)
      .exists(_.nonEmpty)
  }

  /**
   * Check if rule has a valid SQL expression defined
   */
  private def hasSqlExpression(rule: RuleDefinition): Boolean = {
    rule.SQL_expression
      .map(_.trim)
      .exists(_.nonEmpty)
  }

  /**
   * Get evaluation method name (DSL or SQL) for the given rule
   * Useful for logging and debugging
   */
  def getEvaluationMethod(rule: RuleDefinition): String = {
    if (hasDslCondition(rule)) "DSL"
    else if (hasSqlExpression(rule)) "SQL"
    else "NONE"
  }

  /**
   * Get evaluation details for the given rule
   */
  def getEvaluationDetails(rule: RuleDefinition): String = {
    val method = getEvaluationMethod(rule)
    val condition = method match {
      case "DSL" => rule.DSL_condition.map(_.trim).getOrElse("")
      case "SQL" => rule.SQL_expression.map(_.trim).getOrElse("")
      case _ => ""
    }
    s"Method=$method, Condition=${condition.take(100)}"
  }
}

/**
 * RuleEvaluationEngine companion object for factory methods
 */
object RuleEvaluationEngine {
  private val logger = DualLogger(getClass)

  /**
   * Create a new RuleEvaluationEngine
   */
  def apply(spark: SparkSession): RuleEvaluationEngine = {
    new RuleEvaluationEngine(spark)
  }

  /**
   * Log evaluation strategy for all rules in a sequence
   */
  def logEvaluationStrategy(engine: RuleEvaluationEngine, rules: Seq[RuleDefinition]): Unit = {
    rules.foreach { rule =>
      logger.info(s"[RULE][${rule.rule_id}] ${engine.getEvaluationDetails(rule)}")
    }
  }
}
