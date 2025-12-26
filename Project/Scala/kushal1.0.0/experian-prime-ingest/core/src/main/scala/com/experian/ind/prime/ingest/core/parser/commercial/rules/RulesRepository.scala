package com.experian.ind.prime.ingest.core.parser.commercial.rules

import com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial.RuleDefinition
import com.experian.ind.prime.ingest.core.parser.commercial.util.CommercialConstants
import org.apache.spark.sql.{Dataset, SparkSession, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType}

final class RulesRepository(spark: SparkSession) {
  import spark.implicits._

  // OPTIMIZATION: Pre-computed filter literals to avoid creating them repeatedly
  private lazy val activeStatusLit = lit(CommercialConstants.RULESTATUS_ACTIVE)
  private lazy val sqlEngineLit = lit(CommercialConstants.RULEENGINETYPE_SQL)
  private lazy val dslEngineLit = lit(CommercialConstants.RULEENGINETYPE_DSL)
  private lazy val arrayTypeCastDef = ArrayType(StringType)

  /**
   * OPTIMIZATION: Shared logic for loading rules with a specific engine type
   * Eliminates code duplication between loadActiveSqlRules and loadActiveDslRules
   * 
   * @param path Path to the rules JSON file
   * @param allowedIds Sequence of rule IDs to load
   * @param engineType Engine type to filter (SQL or DSL)
   * @return Dataset of RuleDefinition objects filtered by engine type
   */
  private def loadActiveRulesByEngineType(
      path: String,
      allowedIds: Seq[String],
      engineType: Column
  ): Dataset[RuleDefinition] = {
    val df0 = spark.read
      .option("multiline", "true")
      .json(path)

    // OPTIMIZATION: Check schema once and cache field names
    val fieldNames = df0.schema.fieldNames.toSet
    val isSegmentArray = fieldNames.contains(CommercialConstants.SEGMENT) &&
      df0.schema(CommercialConstants.SEGMENT).dataType.isInstanceOf[ArrayType]
    val isFieldTagArray = fieldNames.contains(CommercialConstants.FIELDTAG) &&
      df0.schema(CommercialConstants.FIELDTAG).dataType.isInstanceOf[ArrayType]

    /**
     * OPTIMIZATION: Efficient column transformation without repeated Column object creation
     */
    def asArray(colName: String, alreadyArray: Boolean): Column = {
      if (alreadyArray) {
        col(colName)
      } else {
        val s = col(colName).cast("string")
        val nullCast = lit(null).cast(arrayTypeCastDef)
        val parsed = from_json(s, arrayTypeCastDef)
        when(s.isNull, nullCast).otherwise(coalesce(parsed, array(s)))
      }
    }

    // OPTIMIZATION: Combine transformations and filter in single chain
    val transformed = df0
      .withColumn(CommercialConstants.SEGMENT, asArray(CommercialConstants.SEGMENT, isSegmentArray))
      .withColumn(CommercialConstants.FIELDTAG, asArray(CommercialConstants.FIELDTAG, isFieldTagArray))

    // OPTIMIZATION: Combine multiple filter operations into single where clause with AND logic
    // This reduces DataFrame partitioning and optimization passes
    val filtered = transformed
      .filter(
        coalesce(lower(col(CommercialConstants.RULESTATUS)), activeStatusLit) === activeStatusLit &&
        lower(col(CommercialConstants.RULEENGINETYPE)) === engineType &&
        col(CommercialConstants.RULEID).isin(allowedIds: _*)
      )
      .orderBy(col(CommercialConstants.RULEPRIORITY), col(CommercialConstants.RULEID))

    filtered.as[RuleDefinition]
  }

  /**
   * Load all ACTIVE SQL rules for a given set of rule IDs.
   * Used by RuleEvaluationEngine which evaluates SQL conditions.
   * 
   * Only loads rules with rule_engine_type = "sql".
   * DSL rules are loaded separately via loadActiveDslRules().
   * 
   * OPTIMIZATION: Delegates to loadActiveRulesByEngineType to eliminate code duplication
   * 
   * @param path Path to the rules JSON file
   * @param allowedIds Sequence of rule IDs to load
   * @return Dataset of RuleDefinition objects (SQL-only)
   */
  def loadActiveSqlRules(path: String, allowedIds: Seq[String]): Dataset[RuleDefinition] =
    loadActiveRulesByEngineType(path, allowedIds, sqlEngineLit)

  /**
   * Load all ACTIVE DSL rules for a given set of rule IDs.
   * Used by RuleEvaluationEngine which evaluates DSL conditions.
   * 
   * Only loads rules with rule_engine_type = "dsl" (e.g., R300001, R500001).
   * SQL rules are loaded separately via loadActiveSqlRules().
   * 
   * OPTIMIZATION: Delegates to loadActiveRulesByEngineType to eliminate code duplication
   * 
   * @param path Path to the rules JSON file
   * @param allowedIds Sequence of rule IDs to load
   * @return Dataset of RuleDefinition objects (DSL-only)
   */
  def loadActiveDslRules(path: String, allowedIds: Seq[String]): Dataset[RuleDefinition] =
    loadActiveRulesByEngineType(path, allowedIds, dslEngineLit)

  /**
   * GENERIC FRAMEWORK: Load ALL active rules (both SQL and DSL) for a given set of rule IDs.
   * 
   * This is the preferred method for loading rules when you need to support both SQL and DSL rules
   * in a single pass without knowing the rule types ahead of time.
   * 
   * Returns:
   *   - All rules where rule_engine_type = "sql" OR rule_engine_type = "dsl"
   *   - Filtered to ACTIVE status only
   *   - Sorted by rule_priority (ascending), then by rule_id (for deterministic ordering)
   * 
   * BENEFITS:
   *   - Single I/O pass (loads rules file once instead of twice)
   *   - Eliminates the need to know rule type beforehand
   *   - Works seamlessly with record-level, segment-level, and field-level rules
   *   - Repository handles all filtering and sorting logic
   * 
   * @param path Path to the rules JSON file
   * @param allowedIds Sequence of rule IDs to load
   * @return Dataset of RuleDefinition objects (both SQL and DSL, pre-sorted by priority)
   */
  def loadActiveRules(path: String, allowedIds: Seq[String]): Dataset[RuleDefinition] = {
    val df0 = spark.read
      .option("multiline", "true")
      .json(path)

    // Check schema once and cache field names
    val fieldNames = df0.schema.fieldNames.toSet
    val isSegmentArray = fieldNames.contains(CommercialConstants.SEGMENT) &&
      df0.schema(CommercialConstants.SEGMENT).dataType.isInstanceOf[ArrayType]
    val isFieldTagArray = fieldNames.contains(CommercialConstants.FIELDTAG) &&
      df0.schema(CommercialConstants.FIELDTAG).dataType.isInstanceOf[ArrayType]

    def asArray(colName: String, alreadyArray: Boolean): Column = {
      if (alreadyArray) {
        col(colName)
      } else {
        val s = col(colName).cast("string")
        val nullCast = lit(null).cast(arrayTypeCastDef)
        val parsed = from_json(s, arrayTypeCastDef)
        when(s.isNull, nullCast).otherwise(coalesce(parsed, array(s)))
      }
    }

    val transformed = df0
      .withColumn(CommercialConstants.SEGMENT, asArray(CommercialConstants.SEGMENT, isSegmentArray))
      .withColumn(CommercialConstants.FIELDTAG, asArray(CommercialConstants.FIELDTAG, isFieldTagArray))

    // Filter for ACTIVE status AND (SQL or DSL engine type)
    val filtered = transformed
      .filter(
        coalesce(lower(col(CommercialConstants.RULESTATUS)), activeStatusLit) === activeStatusLit &&
        (lower(col(CommercialConstants.RULEENGINETYPE)) === sqlEngineLit ||
         lower(col(CommercialConstants.RULEENGINETYPE)) === dslEngineLit) &&
        col(CommercialConstants.RULEID).isin(allowedIds: _*)
      )
      .orderBy(col(CommercialConstants.RULEPRIORITY), col(CommercialConstants.RULEID))

    filtered.as[RuleDefinition]
  }
}
