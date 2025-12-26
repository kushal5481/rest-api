package com.experian.ind.prime.ingest.core.parser.commercial.rules

import com.experian.ind.prime.ingest.core.Util.parser.commercial.DualLogger
import com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial.RuleDefinition
import org.apache.spark.sql.SparkSession

/**
 * RuleEvaluationEngine - Orchestrates rule evaluation using Spark SQL.
 *
 * Evaluation Strategy:
 *   1. If a rule has a SQL_expression defined, it is evaluated by the SqlRuleEvaluator.
 *   2. Otherwise, the rule defaults to PASS.
 *
 * This streamlined approach relies on the power and flexibility of Spark SQL for all validations.
 *
 * Usage:
 *   val engine = new RuleEvaluationEngine(spark)
 *   val passed = engine.evaluateRule(rule, "df_view_name")
 */
class RuleEvaluationEngine(spark: SparkSession) {
  private val logger = DualLogger(getClass)
  private val sqlEvaluator = new SqlRuleEvaluator(spark)

  /**
   * Evaluate a rule using the SQL-only strategy.
   *
   * @param rule The rule definition containing the SQL_expression.
   * @param viewName The name of the Spark view to run the SQL expression against.
   * @return true if the rule passes or has no expression, false if it fails.
   */
  def evaluateRule(rule: RuleDefinition, viewName: String): Boolean = {
    val ruleId = rule.rule_id

    try {
      if (hasSqlExpression(rule)) {
        logger.info(s"[RULE][$ruleId] Evaluating rule using SQL on view '$viewName'")
        return sqlEvaluator.evaluateOnRaw(viewName, rule)
      }

      // Default: No SQL expression defined
      logger.warn(s"[RULE][$ruleId] No SQL_expression defined; defaulting to PASS")
      true

    } catch {
      case ex: Exception =>
        logger.error(s"[RULE][$ruleId] Rule evaluation failed: ${ex.getMessage}", ex)
        false // Fail-safe: reject on any evaluation error
    }
  }

  /**
   * Evaluate a record-level rule using SQL.
   */
  def evaluateRecordRule(rule: RuleDefinition, rawRecordView: String = "df_raw_record"): Boolean = {
    evaluateRule(rule, rawRecordView)
  }

  /**
   * Evaluate a field-level rule using SQL.
   */
  def evaluateFieldRule(rule: RuleDefinition, fieldValueView: String = "df_field_value"): Boolean = {
    evaluateRule(rule, fieldValueView)
  }

  /**
   * Evaluate a segment-level rule using SQL.
   */
  def evaluateSegmentRule(rule: RuleDefinition, segmentView: String = "df_segment"): Boolean = {
    evaluateRule(rule, segmentView)
  }

  /**
   * Check if a rule has a valid SQL expression defined.
   */
  private def hasSqlExpression(rule: RuleDefinition): Boolean = {
    rule.SQL_expression
      .map(_.trim)
      .exists(_.nonEmpty)
  }

  /**
   * Get evaluation details for the given rule.
   */
  def getEvaluationDetails(rule: RuleDefinition): String = {
    val method = if (hasSqlExpression(rule)) "SQL" else "NONE"
    val condition = rule.SQL_expression.map(_.trim).getOrElse("")
    s"Method=$method, Condition=${condition.take(100)}"
  }
}

/**
 * RuleEvaluationEngine companion object for factory methods.
 */
object RuleEvaluationEngine {
  private val logger = DualLogger(getClass)

  /**
   * Create a new RuleEvaluationEngine.
   */
  def apply(spark: SparkSession): RuleEvaluationEngine = {
    new RuleEvaluationEngine(spark)
  }

  /**
   * Log evaluation strategy for all rules in a sequence.
   */
  def logEvaluationStrategy(engine: RuleEvaluationEngine, rules: Seq[RuleDefinition]): Unit = {
    rules.foreach { rule =>
      logger.info(s"[RULE][${rule.rule_id}] ${engine.getEvaluationDetails(rule)}")
    }
  }
}

