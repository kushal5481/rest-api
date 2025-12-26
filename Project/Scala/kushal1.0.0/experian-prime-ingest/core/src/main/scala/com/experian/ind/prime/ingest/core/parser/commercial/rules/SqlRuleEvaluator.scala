package com.experian.ind.prime.ingest.core.parser.commercial.rules

import com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial.RuleDefinition
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.experian.ind.prime.ingest.core.Util.parser.commercial.DualLogger

final class SqlRuleEvaluator(spark: SparkSession) {
  private lazy val logger = DualLogger(getClass)

  private def quoteIdentifier(identifier: String): String =
    s"`${identifier.replace("`", "``")}`"

  /**
   * Check if SQL expression is a top-level statement (SELECT or WITH/CTE)
   */
  private def isTopLevelStatement(expr: String): Boolean = {
    val lower = expr.toLowerCase.trim
    lower.startsWith("select") || lower.startsWith("with")
  }

  /**
   * Parse boolean-like value from query result
   */
  private def parseResultValue(value: Any): Boolean = {
    value match {
      case b: java.lang.Boolean =>
        logger.debug(s"Parsed Boolean result: $b")
        b.booleanValue()
      case s: String =>
        val pass = s.toLowerCase matches "(true|1)"
        logger.debug(s"Parsed String result '$s' as: $pass")
        pass
      case n: java.lang.Number =>
        val pass = n.intValue() != 0
        logger.debug(s"Parsed Numeric result ${n.intValue()} as: $pass")
        pass
      case other =>
        logger.warn(s"Unknown result type: ${if (other != null) other.getClass.getSimpleName else "null"}; treating as failure")
        false
    }
  }

  /**
   * Extract query result value from row (prefers 'ok' column, falls back to first column)
   */
  private def extractResultValue(row: org.apache.spark.sql.Row, ruleId: String): Any = {
    try {
      row.getAs[Any]("ok")
    } catch {
      case _: Throwable =>
        logger.debug(s"[RULE][$ruleId] 'ok' column not found, using first column")
        row.get(0)
    }
  }

  /**
   * Execute SQL query and return DataFrame
   */
  private def executeSqlQuery(expr: String, ruleId: String, dfViewForExpression: String): DataFrame = {
    if (isTopLevelStatement(expr)) {
      logger.debug(s"[RULE][$ruleId] Executing as top-level statement (SELECT/WITH)")
      spark.sql(expr)
    } else {
      // Spark SQL expression mode: evaluate the boolean expression against the provided view.
      // This allows rules like: "is_mandatory = false OR length(trim(field_value)) <= max_field_length"
      // without having to write a full SELECT ... FROM ... statement in rule JSON.
      val viewName = Option(dfViewForExpression).map(_.trim).getOrElse("")
      if (viewName.isEmpty) {
        logger.debug(s"[RULE][$ruleId] Executing as scalar expression (no FROM view provided)")
        spark.sql(s"SELECT (${expr}) AS ok")
      } else {
        logger.debug(s"[RULE][$ruleId] Executing as expression against view='$viewName'")
        spark.sql(s"SELECT (${expr}) AS ok FROM ${quoteIdentifier(viewName)}")
      }
    }
  }

  /**
   * Replace df_raw references with actual view name (case-insensitive)
   */
  private def replaceViewReference(expr: String, actualViewName: String): String = {
    // Replace df_raw with the actual view name (case-insensitive regex)
    expr.replaceAll("(?i)\\bdf_raw\\b", actualViewName)
  }

  /**
   * Evaluate a SQL rule against a specific DataFrame view.
   * The SQL_expression should return TRUE when the data is valid.
   * Automatically replaces 'df_raw' references with the actual view name for per-record evaluation.
   *
   * Additionally supports Spark SQL boolean expressions (non-SELECT/WITH) by evaluating them as:
   *   SELECT (<expression>) AS ok FROM <dfRawView>
   * This enables configuration-only rule additions using `SQL_expression` without requiring code changes.
   * 
   * IMPORTANT FOR SEGMENT PARSING RULES:
   * Rules that use LATERAL VIEW POSEXPLODE to parse segments from RawRecord work correctly when
   * evaluating against a single-record view (df_raw_record). The view substitution ensures:
   * - LATERAL VIEW POSEXPLODE parses segments from the single record's RawRecord
   * - Segment order validation, mandatory field checks, etc. apply only to that record
   * - No cross-record interference or inconsistent comparisons
   * 
   * Returns true if rule passes; false if it fails.
   */
  def evaluateOnRaw(dfRawView: String, rule: RuleDefinition): Boolean = {
    val expr = rule.SQL_expression.getOrElse("")
    if (expr.trim.isEmpty) return true

    val ruleId = rule.rule_id
    val ruleName = rule.rule_name.getOrElse("N/A")

    try {
      logger.debug(s"[RULE][$ruleId] Evaluating rule: $ruleName against view: $dfRawView")

      // Replace df_raw references with actual view name (critical for per-record evaluation)
      // This ensures segment parsing rules (using LATERAL VIEW POSEXPLODE) work on single records
      val adjustedExpr = replaceViewReference(expr, dfRawView)
      
      logger.debug(s"[RULE][$ruleId] Full SQL Expression is : $adjustedExpr")
      
      // Execute SQL query
      val df = executeSqlQuery(adjustedExpr, ruleId, dfRawView)
      val rows = df.head(1)

      if (rows.isEmpty) {
        logger.warn(s"[RULE][$ruleId] No result returned; treating as failure")
        return false
      }

      // Extract and parse result value
      val row = rows.head
      val value = extractResultValue(row, ruleId)
      val result = parseResultValue(value)

      // Log final result
      if (result) {
        logger.info(s"[RULE][$ruleId] PASSED")
      } else {
        logger.warn(s"[RULE][$ruleId] FAILED")
      }

      result

    } catch {
      case ex: org.apache.spark.sql.AnalysisException =>
        // Extract user-friendly error message from analysis errors (unresolved columns, etc.)
        val errorMsg = ex.getMessage.split("\\n").head.replaceAll("^.*\\] ", "")
        logger.error(s"[RULE][$ruleId] SQL Error: $errorMsg")
        logger.debug(s"[RULE][$ruleId] Full error details: ${ex.getMessage}")
        false
      case ex: Exception =>
        // Generic exception handling with concise message
        val errorMsg = ex.getMessage match {
          case null => ex.getClass.getSimpleName
          case msg => msg.split("\\n").head
        }
        logger.error(s"[RULE][$ruleId] SQL evaluation failed: $errorMsg")
        logger.debug(s"[RULE][$ruleId] Exception type: ${ex.getClass.getName}")
        false
      case ex: Throwable =>
        logger.error(s"[RULE][$ruleId] Unexpected error: ${ex.getMessage}")
        false
    }
  }
}

