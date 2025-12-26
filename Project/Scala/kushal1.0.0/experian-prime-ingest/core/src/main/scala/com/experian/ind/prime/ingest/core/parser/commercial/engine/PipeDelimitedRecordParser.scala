package com.experian.ind.prime.ingest.core.parser.commercial.engine

import com.experian.ind.prime.ingest.core.Util.parser.commercial.DualLogger
import com.experian.ind.prime.ingest.core.parser.commercial.rules.{RuleEvaluationEngine, RulesRepository}
import com.experian.ind.prime.ingest.core.parser.commercial.util.{CommercialConstants, CommercialUtility}
import com.experian.ind.prime.ingest.core.shared_models.PipelineContext
import com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial.RuleDefinition
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import scala.util.control.Breaks._

// Case class for segmented output row to replace tuple13
private case class SegmentOutputRow(
  fileLoadId: Long,
  batchId: Int,
  recordId: String,
  recordInsertionTime: String,
  segment_tag: String,
  tokens: Seq[String],
  segment_line: String,
  segment_repeat_id: String,
  error_code: String,
  error_message: String,
  rule_id: String,
  metric_code: String,
  reject_type: String,
  field_tag: String,
  field_value: String
)

// Case class for failed rule result
private case class RuleFailureResult(
  ruleId: String,
  ruleLevel: String,
  ruleAction: String,
  errorCode: String,
  errorMessage: String,
  metricCode: String
)

// Case class for efficient rule lookup across record, segment, and field levels
private case class RulesMapping(
  recordRuleIds: Seq[String],  // Record-level rule IDs from recordStructure.ruleId
  segmentRulesBySegmentTag: Map[String, Seq[String]],  // segmentTag -> Seq[ruleIds] from segment.ruleId
  fieldRulesBySegmentField: Map[(String, Int), Seq[String]],  // (segmentTag, fieldIndex) -> Seq[ruleIds]
  rulesById: Map[String, RuleDefinition]  // All loaded rules by rule_id for O(1) lookup
)

/**
 * PipeDelimitedRecordParser - Utility to parse pipe-delimited files based on configuration
 * @param context PipelineContext containing configuration and Spark session
 */
class PipeDelimitedRecordParser(context: PipelineContext) {
  private lazy val logger = DualLogger(getClass)
  private val repository = new RulesRepository(context.spark)
  private val evaluationEngine = new RuleEvaluationEngine(context.spark)

  /**
   * Build the schema for segment output rows
   */
  private def buildSegmentOutputSchema(): StructType = {
    StructType(Seq(
      StructField("FileLoadID", LongType, nullable = false),
      StructField("BatchID", IntegerType, nullable = false),
      StructField("RecordID", StringType, nullable = false),
      StructField("RecordInsertionTime", StringType, nullable = false),
      StructField("Segment_Tag", StringType, nullable = false),
      StructField("tokens", ArrayType(StringType), nullable = false),
      StructField("segment_line", StringType, nullable = false),
      StructField("Segment_Repeat_Id", StringType, nullable = true),
      StructField("Error_Code", StringType, nullable = true),
      StructField("Error_Message", StringType, nullable = true),
      StructField("Rule_Id", StringType, nullable = true),
      StructField("Metric_Code", StringType, nullable = true),
      StructField("Reject_Type", StringType, nullable = true),
      StructField("Field_Tag", StringType, nullable = true),
      StructField("Field_Value", StringType, nullable = true)
    ))
  }

  /**
   * Convert SegmentOutputRow to Spark Row
   */
  private def toSparkRow(row: SegmentOutputRow): org.apache.spark.sql.Row = {
    org.apache.spark.sql.Row(
      row.fileLoadId,
      row.batchId,
      row.recordId,
      row.recordInsertionTime,
      row.segment_tag,
      row.tokens,
      row.segment_line,
      row.segment_repeat_id,
      row.error_code,
      row.error_message,
      row.rule_id,
      row.metric_code,
      row.reject_type,
      row.field_tag,
      row.field_value
    )
  }

  /**
   * Safely cache a table if not already cached
   */
  private def cacheIfNotCached(spark: org.apache.spark.sql.SparkSession, viewName: String): Unit = {
    if (!spark.catalog.isCached(viewName)) {
      spark.catalog.cacheTable(viewName)
    }
  }

  /**
   * Generic rules loading framework supporting both SQL and DSL rules
   * Automatically handles record, segment, and field-level rules regardless of type
   * 
   * This is a framework that:
   *   1. Extracts all rule IDs from schema (record, segment, field levels)
   *   2. Loads ALL rule types (SQL + DSL) ONCE, avoiding redundant I/O
   *   3. Returns sorted and ACTIVE rules only (filtering handled by repository)
   *   4. Organizes rules by level for efficient O(1) lookup during parsing
   *   5. Works seamlessly with any rule type without type-specific logic
   */
  private def buildRulesMapping(rulePath: String): RulesMapping = {
    
    // === SCHEMA EXTRACTION PHASE ===
    // Extract all rule IDs from schema at record, segment, and field levels
    
    val recordRuleIds = Option(context.fileFormatConfigModel.recordStructure.ruleId)
      .map(_.toSeq)
      .getOrElse(Seq.empty)
    
    logger.info(s"[buildRulesMapping] Extracted ${recordRuleIds.size} record-level rule IDs from schema")
    
    // Extract segment-level rules: segmentTag -> Seq[ruleIds]
    val segmentRulesBySegmentTag = context.fileFormatConfigModel.Segments
      .foldLeft(Map.empty[String, Seq[String]]) { case (acc, segment) =>
        val segmentTag = segment.Segment_Tag.toString.trim
        val segmentRuleIds = Option(segment.ruleId).map(_.toSeq).getOrElse(Seq.empty)
        if (segmentRuleIds.nonEmpty) acc + (segmentTag -> segmentRuleIds) else acc
      }
    
    logger.info(s"[buildRulesMapping] Extracted ${segmentRulesBySegmentTag.size} segments with segment-level rules")
    
    // Extract field-level rules: (segmentTag, fieldIndex) -> Seq[ruleIds]
    val fieldRulesBySegmentField = context.fileFormatConfigModel.Segments
      .foldLeft(Map.empty[(String, Int), Seq[String]]) { case (acc, segment) =>
        val segmentTag = segment.Segment_Tag.toString.trim
        if (segment.Fields != null && segment.Fields.nonEmpty) {
          segment.Fields.zipWithIndex.foldLeft(acc) { case (fieldAcc, (field, fieldIndex)) =>
            val fieldRuleIds = Option(field.ruleId).map(_.toSeq).getOrElse(Seq.empty)
            if (fieldRuleIds.nonEmpty) fieldAcc + ((segmentTag, fieldIndex) -> fieldRuleIds) else fieldAcc
          }
        } else acc
      }
    
    logger.info(s"[buildRulesMapping] Extracted ${fieldRulesBySegmentField.size} field-level rule assignments")
    
    // === RULE COLLECTION PHASE ===
    // Collect all unique rule IDs across all levels
    val allUniqueRuleIds = (recordRuleIds ++ segmentRulesBySegmentTag.values.flatten ++ fieldRulesBySegmentField.values.flatten).distinct
    
    if (allUniqueRuleIds.isEmpty) {
      logger.warn("[buildRulesMapping] No rule IDs found in schema")
      return RulesMapping(Seq.empty, Map.empty, Map.empty, Map.empty)
    }
    
    logger.info(s"[buildRulesMapping] Total unique rule IDs to load: ${allUniqueRuleIds.size}")
    
    // === RULES LOADING PHASE ===
    // Load ALL rules (both SQL and DSL) ONCE from repository using generic loadActiveRules()
    // Repository is responsible for:
    //   - Loading both SQL and DSL rule types in single I/O pass
    //   - Filtering to ACTIVE rules only
    //   - Returning rules sorted by priority (ascending) and rule_id (for deterministic ordering)
    // This eliminates redundant loading and ensures consistency across all rule types
    
    logger.info(s"[buildRulesMapping] Loading all active rules (SQL + DSL) from '${rulePath}'...")
    val rulesDS = repository.loadActiveRules(rulePath, allUniqueRuleIds)
    val rulesById = rulesDS.collect().map(rule => rule.rule_id -> rule).toMap
    
    logger.info(s"[buildRulesMapping] Loaded ${rulesById.size} active rules (already sorted by priority in repository)")
    
    // === RULE FILTERING & MAPPING PHASE ===
    // Filter rule IDs to only include those that were actually loaded
    // This handles cases where a rule ID in schema is not found in rules file or is inactive
    
    val filteredRecordRuleIds = recordRuleIds.filter(rulesById.contains)
    val filteredSegmentRulesBySegmentTag = segmentRulesBySegmentTag
      .mapValues(ids => ids.filter(rulesById.contains))
      .filter(_._2.nonEmpty)  // Remove segments with no active rules
    val filteredFieldRulesBySegmentField = fieldRulesBySegmentField
      .mapValues(ids => ids.filter(rulesById.contains))
      .filter(_._2.nonEmpty)  // Remove fields with no active rules
    
    // Log filtering summary if any rules were filtered out
    if (filteredRecordRuleIds.size < recordRuleIds.size) {
      logger.warn(s"[buildRulesMapping] Filtered out ${recordRuleIds.size - filteredRecordRuleIds.size} record-level rules not found in active rules")
    }
    
    val rulesMapping = RulesMapping(
      recordRuleIds = filteredRecordRuleIds,
      segmentRulesBySegmentTag = filteredSegmentRulesBySegmentTag,
      fieldRulesBySegmentField = filteredFieldRulesBySegmentField,
      rulesById = rulesById
    )
    
    logger.info(s"[buildRulesMapping] FINAL: recordRules=${filteredRecordRuleIds.size}, " +
                s"segmentRules=${filteredSegmentRulesBySegmentTag.size}, " +
                s"fieldRules=${filteredFieldRulesBySegmentField.size}, " +
                s"totalLoaded=${rulesById.size}")
    
    rulesMapping
  }

  /**
   * Calculate hierarchical repeat ID for a segment based on parent-child relationships
   * Logic: Each segment type maintains its own counter under each parent
   * Example:
   *   BS (root) → "1"
   *   RS (1st under BS with repeatId "1") → "1.1"
   *   RS (2nd under BS with repeatId "1") → "1.2"
   *   CR (1st under BS with repeatId "1") → "1.1" (separate counter for CR)
   *   GS (1st under CR with repeatId "1.1") → "1.1.1"
   */
  private def calculateSegmentRepeatId(
    segment_tag: String,
    parentMap: Map[String, String],
    currentRepeatIdByTag: scala.collection.mutable.Map[String, String],
    siblingCount: scala.collection.mutable.Map[String, Int]
  ): String = {
    // Get parent tag from configuration (fetched from JSON schema)
    val parentTag = parentMap.getOrElse(segment_tag, "")
    
    // Determine parent's current repeat ID
    val parentRepeatId = if (parentTag.isEmpty) "" else currentRepeatIdByTag.getOrElse(parentTag, "")
    
    // Create unique key combining parent repeat ID and current segment type
    // This ensures each segment type has its own counter under each parent
    val siblingKey = parentRepeatId + "|" + segment_tag
    
    // Increment counter for this (parentRepeatId, segmentType) combination
    val count = siblingCount.getOrElse(siblingKey, 0) + 1
    siblingCount(siblingKey) = count
    
    // Calculate repeat ID in hierarchical format
    // Root segments (no parent): "1", "2", "3", ...
    // Child segments: parentRepeatId + "." + count (e.g., "1.1", "1.2", "1.1.1")
    val repeatId = if (parentRepeatId.isEmpty) {
      count.toString
    } else {
      s"$parentRepeatId.$count"
    }
    
    // Store this segment's repeat ID for its children to reference
    currentRepeatIdByTag(segment_tag) = repeatId
    
    logger.info(s"[calculateSegmentRepeatId] Calculated repeatId for $segment_tag: parentTag=$parentTag, parentRepeatId=$parentRepeatId, siblingKey=$siblingKey, count=$count, result=$repeatId")
    
    repeatId
  }

  /**
   * Extract and add 49 field tag columns via foldLeft
   */
  private def addFieldTagColumns(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    (1 to 49).foldLeft(df) { (acc, i) =>
      val colName = f"$i%02d_field_tag"
      acc.withColumn(colName, element_at(col("tokens"), lit(i)))
    }
  }

  /**
   * Process header and footer (HD/TS) segments
   */
  private def processHeaderFooterSegments(sourceDF: DataFrame, spark: org.apache.spark.sql.SparkSession, 
                                          regex: String): DataFrame = {
    import org.apache.spark.sql.functions._
    
    logger.info("[processHeaderFooterSegments] Processing header/footer (HD/TS) segments for DF2...")
    val nowTime_HT = CommercialUtility.nowFormatted(CommercialConstants.DATETIMEFORMAT)
    val withTokensHT = sourceDF
      .withColumn("tokens", split(col("RawRecord"), regex))
      .withColumn("Segment_Tag", element_at(col("tokens"), lit(1)))
    val onlyHT = withTokensHT.filter(col("Segment_Tag").isin("HD", "TS"))
    val with49HT = addFieldTagColumns(onlyHT)
    
    val df2HT = with49HT
      .withColumn("Segment_Repeat_Id", lit("0"))
      .withColumn("Field_Tag", lit(null).cast(StringType))
      .withColumn("Field_Value", lit(null).cast(StringType))
      .withColumn("Error_Code", lit(null).cast(StringType))
      .withColumn("Error_Message", lit(null).cast(StringType))
      .withColumn("Rule_Id", lit(null).cast(StringType))
      .withColumn("Metric_Code", lit(null).cast(StringType))
      .withColumn("Reject_Type", lit(null).cast(StringType))
      .withColumn("CreatedAt", lit(nowTime_HT).cast(StringType))
    logger.info(s"Header/footer segment count: ${df2HT.count()}")
    
    df2HT
  }

  /**
   * Evaluate all rules for a record and return failure result if any rule fails
   * 
   * Important: Rules that validate segment structure (e.g., segment order) expect to parse segments
   * from RawRecord using LATERAL VIEW POSEXPLODE. These rules need the SINGLE RECORD view (df_raw_record)
   * to work correctly. The replaceViewReference in SqlRuleEvaluator will substitute df_raw with df_raw_record
   * so the rule logic applies only to the current record being evaluated.
   */
  /**
   * GENERIC FRAMEWORK: Evaluate record-level rules from the pre-loaded RulesMapping
   * 
   * This is a schema-driven, rule-agnostic framework that:
   *   1. Parses the raw record into segments
   *   2. Extracts segment order from recordStructure.recordSegmentOrder in schema
   *   3. Calculates comprehensive context: segment counts, positions, sequences, and statistics
   *   4. Populates RuleContext with ALL available data for DSL evaluation
   *   5. Allows ANY DSL rule to operate on this context without code changes
   * 
   * The framework enables users to:
   *   - Define new rules in JSON with custom DSL conditions
   *   - Reference segment_sequence, expected_segment_order, segment counts/positions
   *   - Combine conditions with AND, OR, NOT operators
   *   - Use functions like LENGTH(), TRIM() for string operations
   * 
   * Example DSL Rules:
   *   - R300001: "segment_sequence.containsAll(expected_segment_order) AND segment_sequence.inOrder(expected_segment_order)"
   *   - Future Rules: Any combination of segment statistics and conditions
   * 
   * @param recordId The record ID being evaluated
   * @param rulesMapping The pre-loaded and indexed rules mapping containing all rule definitions
   * @param rawRecord The raw record string containing all segments
   * @return None if all rules pass, Some(RuleFailureResult) if any rule fails
   */
  private def evaluateRulesForRecord(recordId: String, rulesMapping: RulesMapping, rawRecord: String): Option[RuleFailureResult] = {
    import scala.util.control.Breaks._

    // === PHASE 1: PARSE SEGMENTS ===
    val segmentLines = rawRecord.split("\r?\n").filter(_.trim.nonEmpty)
    
    // === PHASE 2: BUILD ACTUAL SEGMENT SEQUENCE ===
    val segment_sequence = segmentLines.map { segment_line =>
      val tokens = segment_line.split("\\|", -1)
      val segment_tag = if (tokens.nonEmpty) tokens(0).trim else ""
      segment_tag
    }.toList

    // === PHASE 3: CREATE SEGMENT VIEW FOR SQL RULE EVALUATION ===
    // This phase creates a temporary view `record_segments` with the structure (pos, tag).
    // This long-format view is more flexible and powerful for complex SQL-based ordering and structural rules
    // than the previous approach of creating a wide view with many pre-aggregated columns.
    // It supports modern, complex rules (like R300001) that can use window functions over this view.
    try {
      val spark = context.spark
      import spark.implicits._

      val segmentsWithPos = segment_sequence.zipWithIndex.map { case (tag, idx) => (idx + 1, tag) }
      if (segmentsWithPos.nonEmpty) {
        val segmentsDF = segmentsWithPos.toDF("pos", "tag")
        segmentsDF.createOrReplaceTempView("record_segments")
        logger.debug(s"[evaluateRulesForRecord] [RECORD=$recordId] Created view 'record_segments' with ${segmentsWithPos.length} rows for SQL rule evaluation.")
      } else {
        logger.warn(s"[evaluateRulesForRecord] [RECORD=$recordId] No segments found in record, 'record_segments' view will be empty.")
        // Create an empty view to prevent SQL errors for rules that expect it
        val schema = StructType(Seq(StructField("pos", IntegerType, false), StructField("tag", StringType, false)))
        spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], schema).createOrReplaceTempView("record_segments")
      }
    } catch {
      case ex: Throwable =>
        logger.warn(s"[evaluateRulesForRecord] [RECORD=$recordId] Failed to create 'record_segments' view: ${ex.getMessage}")
    }

    var failureResult: Option[RuleFailureResult] = None
    breakable {
      // Process each record-level rule ID in order
      for (ruleId <- rulesMapping.recordRuleIds) {
        // Lookup rule definition from the pre-loaded rules map (O(1) lookup)
        rulesMapping.rulesById.get(ruleId) match {
          case Some(rule) =>
            logger.debug(s"[evaluateRulesForRecord] [RECORD=$recordId] Evaluating ruleId='${rule.rule_id}', name='${rule.rule_name.getOrElse("")}'")
            logger.debug(s"[evaluateRulesForRecord] [RECORD=$recordId][RULE=${rule.rule_id}] Evaluating rule using SQL engine.")

            val passed = evaluationEngine.evaluateRecordRule(rule, "df_raw_record")

            if (!passed) {
              failureResult = Some(RuleFailureResult(
                ruleId = rule.rule_id,
                ruleLevel = rule.rule_level.getOrElse(""),
                ruleAction = rule.rule_action.getOrElse(""),
                errorCode = rule.error_code.getOrElse(""),
                errorMessage = rule.error_message.getOrElse(""),
                metricCode = rule.metric_code.getOrElse("")
              ))
              logger.warn(s"[evaluateRulesForRecord] [RECORD=$recordId][RULE=${rule.rule_id}] FAILED - errorCode='${rule.error_code.getOrElse("")}', severity='${rule.severity.getOrElse("")}'")
              break
            } else {
              logger.debug(s"[evaluateRulesForRecord] [RECORD=$recordId][RULE=${rule.rule_id}] PASSED")
            }
          
          case None =>
            logger.warn(s"[evaluateRulesForRecord] [RECORD=$recordId] Record-level rule '$ruleId' not found in loaded rules mapping")
        }
      }
    }
    failureResult
  }

  /**
   * Get field metadata (name, isMandatory, maxLength, etc.) from schema
   */
  private def getFieldMetadata(segmentTag: String, fieldIndex: Int): Option[Map[String, Any]] = {
    try {
      val segment = context.fileFormatConfigModel.Segments.find(s => s.Segment_Tag.toString.trim == segmentTag)
      segment match {
        case Some(seg) =>
          if (seg.Fields != null && fieldIndex < seg.Fields.length) {
            val field = seg.Fields(fieldIndex)
            val maxLen = Option(field.Field_Maximum_Length).getOrElse(0).asInstanceOf[Int]
            Some(Map(
              "fieldIndex" -> fieldIndex,
              "fieldName" -> field.Field_Name,
              "fieldTag" -> field.Field_Tag,
              "isMandatory" -> field.Field_IsMandatory,
              "maxLength" -> maxLen,
              "fieldPriority" -> field.Field_Priority,
              "fieldOccurrence" -> field.Field_Occurrence,
              "expectedValues" -> Option(field.Field_Expected_Values).map(_.toSeq).getOrElse(Seq.empty)
            ))
          } else {
            None
          }
        case None => None
      }
    } catch {
      case e: Exception =>
        logger.error(s"[getFieldMetadata] Error extracting field metadata for segment='$segmentTag', fieldIndex=$fieldIndex", e)
        None
    }
  }

  /**
   * Create a DataFrame for field-level validation containing field values and metadata
   * This view will be used for evaluating field-level rules like R500001
   */
  private def createFieldValidationView(recordId: String, segmentTag: String, fieldIndex: Int, fieldValue: String, 
                                       fieldMetadata: Map[String, Any], spark: org.apache.spark.sql.SparkSession): Unit = {
    
    val isMandatory = fieldMetadata.getOrElse("isMandatory", false).asInstanceOf[Boolean]
    val maxLength = fieldMetadata.getOrElse("maxLength", 0).asInstanceOf[Int]
    val fieldName = fieldMetadata.getOrElse("fieldName", "").asInstanceOf[String]
    val fieldTag = fieldMetadata.getOrElse("fieldTag", "").asInstanceOf[String]
    
    //logger.debug(s"[RECORD=$recordId][SEGMENT=$segmentTag][FIELD_INDEX=$fieldIndex] Schema field: name='$fieldName', tag='$fieldTag', isMandatory=$isMandatory, maxLength=$maxLength")
    //logger.debug(s"[RECORD=$recordId][SEGMENT=$segmentTag][FIELD_INDEX=$fieldIndex] Actual field value: '$fieldValue' (length=${fieldValue.length}) - Comparison: LENGTH($fieldValue) <= $maxLength = ${fieldValue.length <= maxLength}")
    
    // Create a single-row DataFrame for field validation
    val fieldValidationData = Seq(
      (recordId, segmentTag, fieldIndex, fieldTag, fieldName, fieldValue, isMandatory, maxLength)
    )
    
    val fieldValidationDf = spark.createDataFrame(fieldValidationData)
      .toDF("record_id", "segment_tag", "field_index", "field_tag", "field_name", "field_value", "is_mandatory", "max_field_length")
    
    fieldValidationDf.createOrReplaceTempView("df_field_value")
    //logger.debug(s"[RECORD=$recordId][SEGMENT=$segmentTag][FIELD_INDEX=$fieldIndex] Created df_field_value: is_mandatory=$isMandatory, max_field_length=$maxLength, field_value_length=${fieldValue.length}")
  }

  /**
   * Evaluate field-level rules for a specific field using pre-loaded RulesMapping
   * Fetches rules by key (segmentTag, fieldIndex) from fieldRulesBySegmentField
   * Returns failure result if any field-level rule fails
   * 
   * @param recordId The record ID being evaluated
   * @param segmentTag The segment tag (e.g., "BS", "RS", "CR")
   * @param fieldIndex The zero-based field index within the segment
   * @param fieldValue The actual field value from the parsed segment
   * @param fieldMetadata Field metadata (mandatory flag, max length, etc.)
   * @param rulesMapping The pre-loaded rules mapping with field rules indexed by (segmentTag, fieldIndex)
   * @param spark The Spark session for creating temporary views
   * @return None if all rules pass, Some(RuleFailureResult) if any rule fails
   */
  private def evaluateRulesForField(recordId: String, segmentTag: String, fieldIndex: Int, fieldValue: String,
                                 fieldMetadata: Map[String, Any], rulesMapping: RulesMapping, 
                                 spark: org.apache.spark.sql.SparkSession): Option[RuleFailureResult] = {
    import scala.util.control.Breaks._
    
    val fieldName = fieldMetadata.getOrElse("fieldName", "").asInstanceOf[String]
    val isMandatory = fieldMetadata.getOrElse("isMandatory", false).asInstanceOf[Boolean]
    val maxLength = fieldMetadata.getOrElse("maxLength", 0).asInstanceOf[Int]
    
    // Lookup field-level rule IDs for this specific field using the key (segmentTag, fieldIndex)
    val fieldRuleIds = rulesMapping.fieldRulesBySegmentField.getOrElse((segmentTag, fieldIndex), Seq.empty)

    if (fieldRuleIds.isEmpty) {
      logger.debug(s"[RECORD=$recordId][SEGMENT=$segmentTag][FIELD_INDEX=$fieldIndex] No field-level rules configured")
      return None
    }

    // Create field validation view for this field
    createFieldValidationView(recordId, segmentTag, fieldIndex, fieldValue, fieldMetadata, spark)

    var failureResult: Option[RuleFailureResult] = None
    breakable {
      for (ruleId <- fieldRuleIds) {
        // Lookup rule definition from the pre-loaded rules map (O(1) lookup)
        rulesMapping.rulesById.get(ruleId) match {
          case Some(rule) =>
            // Check if this rule should be applied for the current segment and field_tag
            val allowedSegments: Seq[String] = rule.segment.map(_.map(_.toString.trim)).getOrElse(Seq.empty)
            val allowedFieldTags: Seq[String] = rule.field_tag.map(_.map(_.toString.trim)).getOrElse(Seq.empty)
            val fieldTag = fieldMetadata.getOrElse("fieldTag", "").asInstanceOf[String]

            val segmentMatch = allowedSegments.isEmpty || allowedSegments.contains(segmentTag)
            val fieldTagMatch = allowedFieldTags.isEmpty || allowedFieldTags.contains(fieldTag)

            if (!segmentMatch || !fieldTagMatch) {
              logger.debug(s"[RECORD=$recordId][SEGMENT=$segmentTag][FIELD=$fieldName][RULE=$ruleId] Skipping rule: segmentMatch=$segmentMatch, fieldTagMatch=$fieldTagMatch, allowedSegments=[${allowedSegments.mkString(",")}], allowedFieldTags=[${allowedFieldTags.mkString(",")}], fieldTag=$fieldTag")
              // Skip this rule for this segment/field
            } else {
              // Skip field-level rule evaluation if field is not mandatory and rule requires mandatory check
              val ruleType = rule.rule_type.getOrElse("")
              if (ruleType == "MANDATORY" && !isMandatory) {
                logger.debug(s"[RECORD=$recordId][SEGMENT=$segmentTag][FIELD=$fieldName][RULE=$ruleId] Skipping mandatory-type rule for non-mandatory field")
                // Continue to next rule
              } else {
                val passed = evaluationEngine.evaluateFieldRule(rule, "df_field_value")

                if (!passed) {
                  failureResult = Some(RuleFailureResult(
                    ruleId = rule.rule_id,
                    ruleLevel = rule.rule_level.getOrElse(""),
                    ruleAction = rule.rule_action.getOrElse(""),
                    errorCode = rule.error_code.getOrElse(""),
                    errorMessage = rule.error_message.getOrElse(""),
                    metricCode = rule.metric_code.getOrElse("")
                  ))
                  logger.warn(s"[RECORD=$recordId][SEGMENT=$segmentTag][FIELD=$fieldName][RULE=$ruleId] FAILED - errorCode='${rule.error_code.getOrElse("")}'")
                  break
                } else {
                  logger.debug(s"[RECORD=$recordId][SEGMENT=$segmentTag][FIELD=$fieldName][RULE=$ruleId] PASSED")
                }
              }
            }
          case None =>
            logger.warn(s"[RECORD=$recordId][SEGMENT=$segmentTag][FIELD=$fieldName] Field-level rule '$ruleId' not found in loaded rules mapping")
        }
      }
    }

    failureResult
  }

  /**
   * Get segment metadata from schema configuration
   * @param segmentTag Segment tag (e.g., "BS", "RS", "CR", "GS", "HD", "TS")
   * @return Map containing segment metadata: isMandatory, delimiterCount, expectedValues, name, etc.
   */
  private def getSegmentMetadata(segmentTag: String): Option[Map[String, Any]] = {
    context.fileFormatConfigModel.Segments.find(_.Segment_Tag.toString.trim == segmentTag).map { segment =>
      Map(
        "segmentName" -> segment.Segment_Name,
        "isMandatory" -> segment.Segment_IsMandatory,
        "delimiterCount" -> segment.Segement_Delimiter_Count,
        "expectedValues" -> segment.Segment_Expected_Values,
        "maxLength" -> segment.Segment_Maximum_Length,
        "priority" -> segment.Segment_Priority,
        "typeOfLine" -> segment.Segment_TypeofLine
      )
    }
  }

  /**
   * Evaluate segment-level rules for a given segment
   * Segment-level rules like R400001 (SEGMENT_DELIMITER_COUNT_MISMATCH) validate segment structure
   * @param recordId Record ID for logging
   * @param segmentTag Segment tag (e.g., "BS", "RS", "CR", "GS")
   * @param segmentLine Raw segment line text
   * @param rulesMapping Pre-loaded rules mapping with segment-level rules
   * @param spark Spark session for rule evaluation
   * @return Option[RuleFailureResult] if any segment-level rule fails, None if all pass
   */
  private def evaluateRulesForSegment(recordId: String, segmentTag: String, segmentLine: String, 
                                     rulesMapping: RulesMapping, 
                                     spark: org.apache.spark.sql.SparkSession): Option[RuleFailureResult] = {
    import scala.util.control.Breaks._
    
    // Lookup segment-level rule IDs for this specific segment tag
    val segmentRuleIds = rulesMapping.segmentRulesBySegmentTag.getOrElse(segmentTag, Seq.empty)
    
    if (segmentRuleIds.isEmpty) {
      logger.debug(s"[RECORD=$recordId][SEGMENT=$segmentTag] No segment-level rules configured")
      return None
    }
    
    // Get segment metadata from schema (mandatory, delimiter count, expected values, etc.)
    val segmentMetadataOpt = getSegmentMetadata(segmentTag)
    
    if (segmentMetadataOpt.isEmpty) {
      logger.warn(s"[RECORD=$recordId][SEGMENT=$segmentTag] Segment metadata not found in schema")
      return None
    }
    
    val segmentMetadata = segmentMetadataOpt.get
    val isMandatory = segmentMetadata.getOrElse("isMandatory", false).asInstanceOf[Boolean]
    val delimiterCount = segmentMetadata.getOrElse("delimiterCount", 0).asInstanceOf[Int]
    val expectedValues = segmentMetadata.getOrElse("expectedValues", List[String]()).asInstanceOf[List[String]]
    
    // Count actual delimiters in the segment
    val actualDelimiterCount = segmentLine.count(_ == '|')
    
    logger.debug(s"[RECORD=$recordId][SEGMENT=$segmentTag] Segment metadata: isMandatory=$isMandatory, expectedDelimiterCount=$delimiterCount, actualDelimiterCount=$actualDelimiterCount")

    // Create a single-row DataFrame for segment validation and register it as a temporary view.
    // This view is used by the SQL rule engine to evaluate segment-level rules.
    val segmentValidationData = Seq(
      (recordId, segmentTag, segmentLine, isMandatory, delimiterCount, actualDelimiterCount)
    )
    val segmentValidationDf = spark.createDataFrame(segmentValidationData)
      .toDF("record_id", "segment_tag", "segment_line", "is_mandatory", "expected_delimiter_count", "actual_delimiter_count")
    segmentValidationDf.createOrReplaceTempView("df_segment")
    
    var failureResult: Option[RuleFailureResult] = None
    breakable {
      for (ruleId <- segmentRuleIds) {
        // Lookup rule definition from the pre-loaded rules map (O(1) lookup)
        rulesMapping.rulesById.get(ruleId) match {
          case Some(rule) =>
            // Explicit segment filter: only apply rule if segmentTag is in rule.segment, or if rule.segment is empty/missing
            val allowedSegments = rule.segment match {
              case Some(seq) => seq.map(s => if (s != null) s.trim else "").filter(_.nonEmpty)
              case None => Seq.empty[String]
            }
            if (allowedSegments.nonEmpty && !allowedSegments.contains(segmentTag)) {
              logger.debug(s"[RECORD=$recordId][SEGMENT=$segmentTag][RULE=$ruleId] Skipping rule: segment not in rule.segment list")
              // Skip this rule for this segment
              // continue to next rule
            } else {
            logger.debug(s"[RECORD=$recordId][SEGMENT=$segmentTag][RULE=$ruleId] Evaluating rule='${rule.rule_name.getOrElse("")}'")
            logger.debug(s"[RECORD=$recordId][SEGMENT=$segmentTag][RULE=$ruleId] Evaluating rule using SQL engine.")
            
             val passed = evaluationEngine.evaluateSegmentRule(rule, "df_segment")
             
             if (!passed) {
              failureResult = Some(RuleFailureResult(
                ruleId = rule.rule_id,
                ruleLevel = rule.rule_level.getOrElse(""),
                ruleAction = rule.rule_action.getOrElse(""),
                errorCode = rule.error_code.getOrElse(""),
                errorMessage = rule.error_message.getOrElse(""),
                metricCode = rule.metric_code.getOrElse("")
              ))
              logger.warn(s"[RECORD=$recordId][SEGMENT=$segmentTag][RULE=$ruleId] FAILED - errorCode='${rule.error_code.getOrElse("")}'")
              break
            } else {
              logger.debug(s"[RECORD=$recordId][SEGMENT=$segmentTag][RULE=$ruleId] PASSED")
            }
            }
          
          case None =>
            logger.warn(s"[RECORD=$recordId][SEGMENT=$segmentTag] Segment-level rule '$ruleId' not found in loaded rules mapping")
        }
      }
    }
    
    failureResult
  }

  /**
   * Parse the source DataFrame according to pipe-delimited format
   * @param sourceDF Source DataFrame to be parsed
   * @return Parsed DataFrame
   */
  def parseFile(sourceDF: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    val spark = context.spark
    spark.conf.set("spark.sql.debug.maxToStringFields", 10000)  // Increase limit to avoid truncation warnings
    val delimiter = Option(context.fileFormatConfigModel.fileStructure.Delimiter).getOrElse("|")
    val regex = java.util.regex.Pattern.quote(delimiter)

    logger.info(s"Starting parseFile for DF2. Input DataFrame count: ${sourceDF.count()}, columns: ${sourceDF.columns.mkString(",")}")

    // --- HEADER/FOOTER (HD/TS) ---
    val df2HT = processHeaderFooterSegments(sourceDF, spark, regex)

    // --- RECORD SPLIT ---
    // Normalize parent tags from JSON (handles Option/Some and nulls)
    def normalizeParent(p: Any): String = {
      val raw = Option(p).map(_.toString).getOrElse("")
      val cleaned = if (raw.startsWith("Some(") && raw.endsWith(")")) raw.substring(5, raw.length - 1) else raw
      cleaned.trim
    }
    val parentMap: Map[String, String] = context.fileFormatConfigModel.Segments
      .map(s => s.Segment_Tag.toString.trim -> normalizeParent(s.Segment_Parent))
      .toMap
    
    val rulePath = context.fileConfigModel.ruleLocation
    if (rulePath == null || rulePath.trim.isEmpty) {
      throw new IllegalArgumentException("ruleLocation not found in file-config.json")
    }

    // 1) Register sourceDF as df_raw view for SQL rule evaluation (required by evaluateOnRaw)
    sourceDF.createOrReplaceTempView("df_raw")
    cacheIfNotCached(spark, "df_raw")

    // 2) Build efficient rules mapping: load ALL rules (record + field) ONCE at the beginning
    // This structure enables efficient O(1) lookup for both record-level and field-level rules
    val rulesMapping = buildRulesMapping(rulePath)
    
    // Validate that we have at least record-level rules
    if (rulesMapping.recordRuleIds.isEmpty) {
      logger.warn("[RULES] No record-level rules configured; returning empty DF2")
      val empty = context.spark.emptyDataFrame
      return empty
    }

    // Get all unique Record IDs for per-record processing
    val recordIds = sourceDF
      .filter(col("Type_of_Line") === "Record")
      .select("RecordID")
      .distinct()
      .collect()
      .map(_.getString(0))
      .toSeq
    
    logger.info(s"Processing ${recordIds.size} records with per-record rule evaluation")

    // Process each record individually with rule evaluation
    val recordOutputRows = recordIds.flatMap { recordId =>
      logger.debug(s"[RECORD=$recordId] ===== Processing Record =====")
      
      // Create temporary view with just this record
      val recordDf = sourceDF.filter(col("RecordID") === recordId)
      recordDf.createOrReplaceTempView("df_raw_record")
      
      // Get the raw data for this record
      val recordRows = recordDf.collect()
      if (recordRows.isEmpty) {
        logger.warn(s"[RECORD=$recordId] No rows found for record")
        Seq.empty
      } else {
        val recordRow = recordRows.head
        val fileLoadId = recordRow.getAs[Long]("FileLoadID")
        val batchId = recordRow.getAs[Int]("BatchID")
        val recordInsertionTime = recordRow.getAs[String]("RecordInsertionTime")
        val rawRecord = recordRow.getAs[String]("RawRecord")
        
        // Evaluate record-level rules using the pre-loaded rulesMapping and passing rawRecord
        val failureOpt = evaluateRulesForRecord(recordId, rulesMapping, rawRecord)
        
        logger.debug(s"[RECORD=$recordId] recordFailed=${failureOpt.isDefined}, rawRecord length=${rawRecord.length}")
        
        // Parse segments and assign repeat IDs based on rule result
        val segmentLines = rawRecord.split("\r?\n").filter(_.trim.nonEmpty)
        logger.debug(s"[RECORD=$recordId] Found ${segmentLines.length} segments")
        
        // Calculate repeat IDs based on rule result
        failureOpt match {
          case Some(failure) =>
            // Rule failed: still calculate hierarchical repeat IDs but populate error information in all segments
            logger.debug(s"[RECORD=$recordId] Record-level rule failed; calculating repeat IDs and applying error info to all segments")
            
            // OPTIMIZATION: Pre-calculate segment tags to avoid redundant splits
            val segmentData = segmentLines.map { segment_line =>
              val tokens = segment_line.split("\\|", -1)
              val segment_tag = if (tokens.nonEmpty) tokens(0).trim else ""
              (segment_line, tokens, segment_tag)
            }
            
            val (_, _, outputRows) = segmentData.foldLeft(
              (scala.collection.mutable.Map[String, Int](), scala.collection.mutable.Map[String, String](), scala.collection.mutable.ArrayBuffer[SegmentOutputRow]())
            ) { case ((siblingCount, currentRepeatIdByTag, rowBuffer), (segment_line, tokens, segment_tag)) =>
              
              // Calculate repeat ID using the hierarchical logic (even when rule fails)
              val repeatId = calculateSegmentRepeatId(segment_tag, parentMap, currentRepeatIdByTag, siblingCount)
              
              val row = SegmentOutputRow(
                fileLoadId = fileLoadId,
                batchId = batchId,
                recordId = recordId,
                recordInsertionTime = recordInsertionTime,
                segment_tag = segment_tag,
                tokens = tokens.toSeq,
                segment_line = segment_line,
                segment_repeat_id = repeatId,  // Use calculated repeat ID, not error code
                error_code = failure.errorCode,
                error_message = failure.errorMessage,
                rule_id = failure.ruleId,
                metric_code = failure.metricCode,
                reject_type = failure.ruleAction + CommercialConstants.UNDERSCORESIGN + failure.ruleLevel,
                field_tag = null,
                field_value = null
              )
              
              rowBuffer += row
              (siblingCount, currentRepeatIdByTag, rowBuffer)
            }
            
            outputRows
          
          case None =>
            // Rule passed: calculate proper hierarchical repeat IDs and evaluate field-level rules
            logger.debug(s"[RECORD=$recordId] Calculating hierarchical repeat IDs for all segments using Segment_Parent from schema")
            
            // OPTIMIZATION: Pre-calculate segment tags to avoid redundant splits
            val segmentData = segmentLines.map { segment_line =>
              val tokens = segment_line.split("\\|", -1)
              val segment_tag = if (tokens.nonEmpty) tokens(0).trim else ""
              (segment_line, tokens, segment_tag)
            }
            
            val (_, _, outputRows) = segmentData.foldLeft(
              (scala.collection.mutable.Map[String, Int](), scala.collection.mutable.Map[String, String](), scala.collection.mutable.ArrayBuffer[SegmentOutputRow]())
            ) { case ((siblingCount, currentRepeatIdByTag, rowBuffer), (segment_line, tokens, segment_tag)) =>
              
              // Calculate repeat ID using the hierarchical logic (fetches parent from JSON)
              val repeatId = calculateSegmentRepeatId(segment_tag, parentMap, currentRepeatIdByTag, siblingCount)
              
              // Evaluate segment-level rules for this segment (BEFORE field-level rules)
              // Segment-level rules check segment structure like delimiter count (R400001)
              val segmentFailureOpt = evaluateRulesForSegment(recordId, segment_tag, segment_line, rulesMapping, spark)
              
              // If segment-level rule failed, report error and skip field-level evaluation
              val (fieldFailureOpt, failingFieldIndex, failingFieldValue) = if (segmentFailureOpt.isDefined) {
                logger.warn(s"[RECORD=$recordId][SEGMENT=$segment_tag] Segment-level rule failed; skipping field-level evaluation")
                (segmentFailureOpt, -1, null)
              } else {
                // Segment-level rules passed: proceed to evaluate field-level rules for each field in this segment
                // OPTIMIZATION: Build complete field metadata map once per segment to avoid repeated schema lookups
                val fieldMetadataMap = scala.collection.mutable.Map[Int, Map[String, Any]]()
                for (fieldIdx <- 0 until tokens.length) {
                  getFieldMetadata(segment_tag, fieldIdx).foreach { metadata =>
                    fieldMetadataMap(fieldIdx) = metadata
                  }
                }
                
                var fieldFailure: Option[RuleFailureResult] = None
                var failingIdx: Int = -1
                var failingVal: String = null
                breakable {
                  for (tokenIndex <- 0 until tokens.length) {  // Start from 0 to include segment tag
                    val fieldValue = tokens(tokenIndex)
                    val fieldIndex = tokenIndex  // Direct mapping: tokens[0] -> Field_Index 0, tokens[1] -> Field_Index 1, etc.
                    
                    fieldMetadataMap.get(fieldIndex) match {
                      case Some(metadata) =>
                        // Evaluate field-level rules for this field using the pre-loaded rulesMapping
                        // Rules are fetched by key (segmentTag, fieldIndex) and executed in priority order
                        // All fields, including Field_Index=0 (Segment Identifier), are validated
                        val fieldRuleFailure = evaluateRulesForField(recordId, segment_tag, fieldIndex, fieldValue, 
                                                                   metadata, rulesMapping, spark)
                        
                        if (fieldRuleFailure.isDefined) {
                          fieldFailure = fieldRuleFailure
                          failingIdx = fieldIndex
                          failingVal = fieldValue
                          logger.warn(s"[RECORD=$recordId][SEGMENT=$segment_tag][FIELD_INDEX=$fieldIndex] Field-level rule failed")
                          break
                        }
                      
                      case None =>
                        // OPTIMIZATION: Debug logging for missing field metadata
                        logger.debug(s"[RECORD=$recordId][SEGMENT=$segment_tag][FIELD_INDEX=$fieldIndex] No metadata found for field")
                    }
                  }
                }
                (fieldFailure, failingIdx, failingVal)
              }
              
              // Create output row with or without field-level rule failure
              val (repeatIdForRow, errorCode, errorMsg, ruleId, metricCode, rejectType, fieldTag, fieldValue) = fieldFailureOpt match {
                case Some(failure) =>
                  val ft = if (failingFieldIndex >= 0) f"${failingFieldIndex + 1}%02d_field_tag" else null
                  val fv = if (failingFieldIndex >= 0) failingFieldValue else null
                  (repeatId, failure.errorCode, failure.errorMessage, failure.ruleId, failure.metricCode, failure.ruleAction + CommercialConstants.UNDERSCORESIGN + failure.ruleLevel, ft, fv)
                case None =>
                  (repeatId, null, null, null, null, null, null, null)
              }
              
              val row = SegmentOutputRow(
                fileLoadId = fileLoadId,
                batchId = batchId,
                recordId = recordId,
                recordInsertionTime = recordInsertionTime,
                segment_tag = segment_tag,
                tokens = tokens.toSeq,
                segment_line = segment_line,
                segment_repeat_id = repeatIdForRow,
                error_code = errorCode,
                error_message = errorMsg,
                rule_id = ruleId,
                metric_code = metricCode,
                reject_type = rejectType,
                field_tag = fieldTag,
                field_value = fieldValue
              )
              
              rowBuffer += row
              (siblingCount, currentRepeatIdByTag, rowBuffer)
            }
            
            outputRows
        }
      }
    }
    
    logger.info(s"Per-record evaluation generated ${recordOutputRows.size} segment rows")

    // Convert output rows to RDD and create DataFrame
    val rowRdd = spark.sparkContext.parallelize(
      recordOutputRows.map(toSparkRow)
    )
    val explodedRows = spark.createDataFrame(rowRdd, buildSegmentOutputSchema())

    val with49 = addFieldTagColumns(explodedRows)
    val nowTime_Record = CommercialUtility.nowFormatted(CommercialConstants.DATETIMEFORMAT)
    
    // Apply error codes per record (already set in recordOutputRows, just cast columns properly)
    val df2Rec = with49
      .withColumn("Type_of_Line", lit("Record"))
      .withColumn("CreatedAt", lit(nowTime_Record).cast(StringType))
    logger.info(s"Record segment count: ${df2Rec.count()}")

    // --- UNION ALL ---
    val tagCols = (1 to 49).map(i => col(f"$i%02d_field_tag"))
    val selectedCols = Seq(
      col("FileLoadID"),
      col("BatchID"),
      col("RecordID"),
      col("Type_of_Line"),
      col("Segment_Tag"),
      col("Segment_Repeat_Id")
    ) ++ tagCols ++ Seq(
      col("Field_Tag"),
      col("Field_Value"),
      col("Error_Code"),
      col("Error_Message"),
      col("Rule_Id"),
      col("Metric_Code"),
      col("Reject_Type"),
      col("CreatedAt")
    )

    val df2 = df2HT.select(selectedCols: _*).unionByName(df2Rec.select(selectedCols: _*))
    logger.info(s"DF2 final row count: ${df2.count()}")

    CommercialUtility.writeDataFrame(context, context.parserConfigModel.dataFrames.dataframe2, df2)
    
    // --- CLEANUP TEMPORARY VIEWS AND CACHED DATA ---
    CommercialUtility.cleanupTemporaryViews(spark, List("df_raw", "df_raw_record", "df_field_value", "df_segment"))
    
    df2
  }
}
