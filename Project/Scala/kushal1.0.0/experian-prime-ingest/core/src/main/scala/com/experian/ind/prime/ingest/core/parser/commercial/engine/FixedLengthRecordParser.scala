package com.experian.ind.prime.ingest.core.parser.commercial.engine

import com.experian.ind.prime.ingest.core.Util.parser.commercial.DualLogger
import com.experian.ind.prime.ingest.core.parser.commercial.rules.{RuleEvaluationEngine, RulesRepository}
import com.experian.ind.prime.ingest.core.parser.commercial.util.{CommercialConstants, CommercialUtility}
import com.experian.ind.prime.ingest.core.shared_models.PipelineContext
import com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial.RuleDefinition
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

import scala.util.control.Breaks._
import scala.collection.mutable
import scala.collection.JavaConverters._

// Case class for segment output row (same structure as PipeDelimitedParser)
private case class FixedLengthSegmentOutputRow(
  fileLoadId: Long,
  batchId: Int,
  recordId: String,
  recordInsertionTime: String,
  segment_tag: String,
  field_tag: String,
  field_value: String,
  segment_repeat_id: String,
  error_code: String,
  error_message: String,
  rule_id: String,
  metric_code: String,
  reject_type: String
)

// Case class for failed rule result
private case class FixedLengthRuleFailureResult(
  ruleId: String,
  ruleLevel: String,
  ruleAction: String,
  errorCode: String,
  errorMessage: String,
  metricCode: String
)

// Case class for efficient rule lookup
private case class FixedLengthRulesMapping(
  recordRuleIds: Seq[String],
  segmentRulesBySegmentTag: Map[String, Seq[String]],
  fieldRulesBySegmentField: Map[(String, Int), Seq[String]],
  rulesById: Map[String, RuleDefinition]
)

/**
 * FixedLengthRecordParser - Utility to parse fixed-length files based on configuration
 * 
 * Data Format:
 * - Each segment is on a separate line (delimited by \r?\n)
 * - Each field within a segment is extracted using fixed-length positions
 * - Field positions and lengths are defined in the schema (BTUDF10_FixedLength.json)
 * - Examples:
 *   * HD segment: 90 characters total (Segment Tag 0-4, Member ID 5-14, etc.)
 *   * BS segment: 1400 characters total (Segment Tag 0-1, Branch Code 2-16, etc.)
 *   * CR segment: Multiple fields at fixed positions
 * 
 * @param context PipelineContext containing configuration and Spark session
 */
class FixedLengthRecordParser(context: PipelineContext) {
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
      StructField("Field_Tag", StringType, nullable = true),
      StructField("Field_Value", StringType, nullable = true),
      StructField("Segment_Repeat_Id", StringType, nullable = true),
      StructField("Error_Code", StringType, nullable = true),
      StructField("Error_Message", StringType, nullable = true),
      StructField("Rule_Id", StringType, nullable = true),
      StructField("Metric_Code", StringType, nullable = true),
      StructField("Reject_Type", StringType, nullable = true)
    ))
  }

  /**
   * Convert FixedLengthSegmentOutputRow to Spark Row
   */
  private def toSparkRow(row: FixedLengthSegmentOutputRow): org.apache.spark.sql.Row = {
    org.apache.spark.sql.Row(
      row.fileLoadId,
      row.batchId,
      row.recordId,
      row.recordInsertionTime,
      row.segment_tag,
      row.field_tag,
      row.field_value,
      row.segment_repeat_id,
      row.error_code,
      row.error_message,
      row.rule_id,
      row.metric_code,
      row.reject_type
    )
  }

  /**
   * Generic rules loading framework supporting both SQL and DSL rules for fixed-length parsing
   * Automatically handles record, segment, and field-level rules regardless of type
   * 
   * This is a framework that:
   *   1. Extracts all rule IDs from schema (record, segment, field levels)
   *   2. Loads ALL rule types (SQL + DSL) ONCE, avoiding redundant I/O
   *   3. Returns sorted and ACTIVE rules only (filtering handled by repository)
   *   4. Organizes rules by level for efficient O(1) lookup during parsing
   *   5. Works seamlessly with any rule type without type-specific logic
   */
  private def buildRulesMapping(rulePath: String): FixedLengthRulesMapping = {
    
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
      return FixedLengthRulesMapping(Seq.empty, Map.empty, Map.empty, Map.empty)
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
    
    val rulesMapping = FixedLengthRulesMapping(
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
   * Add 49 field tag columns to DataFrame (columns 01_field_tag through 49_field_tag)
   * These columns will contain the field tag values for each position (or null if not present in segment)
   * This matches the output structure expected by downstream systems
   */
  private def addFieldTagColumns(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    // For fixed-length, we extract field tags from segment metadata based on segment_tag
    // and populate them as separate columns
    (1 to 49).foldLeft(df) { (acc, i) =>
      val colName = f"$i%02d_field_tag"
      // Create a null column for now - will be populated per segment if needed
      acc.withColumn(colName, lit(null).cast(StringType))
    }
  }

  /**
   * Process header and footer (HD/TS) segments for fixed-length format
   * For fixed-length: segment tag is extracted from fixed position (first 2 characters)
   * Header/footer segments are processed separately and added with Type_of_Line = "Header"/"Footer"
   */
  private def processHeaderFooterSegments(sourceDF: DataFrame, spark: org.apache.spark.sql.SparkSession): DataFrame = {
    import org.apache.spark.sql.functions._
    
    logger.info("[processHeaderFooterSegments] Processing header/footer (HD/TS) segments for DF2 (fixed-length format)...")
    val nowTime_HT = CommercialUtility.nowFormatted(CommercialConstants.DATETIMEFORMAT)
    
    // For fixed-length: Extract segment tag from fixed position (characters 1-2, 1-based indexing)
    val withSegmentTag = sourceDF
      .withColumn("Segment_Tag", substring(col("RawRecord"), 1, 2))
    
    val onlyHT = withSegmentTag.filter(col("Segment_Tag").isin("HD", "TS"))
    val with49HT = addFieldTagColumns(onlyHT)
    
    val df2HT = with49HT
      .withColumn("Type_of_Line", lit("Header"))  // Mark as header/footer
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
   * Get segment metadata from schema configuration for fixed-length parsing
   * @param segmentTag Segment tag (e.g., "BS", "RS", "CR", "GS", "HD", "TS")
   * @return Map containing segment metadata: isMandatory, maxLength, name, etc.
   */
  private def getSegmentMetadata(segmentTag: String): Option[Map[String, Any]] = {
    context.fileFormatConfigModel.Segments.find(_.Segment_Tag.toString.trim == segmentTag).map { segment =>
      Map(
        "segmentName" -> segment.Segment_Name,
        "isMandatory" -> segment.Segment_IsMandatory,
        "maxLength" -> segment.Segment_Maximum_Length,
        "priority" -> segment.Segment_Priority,
        "typeOfLine" -> segment.Segment_TypeofLine
      )
    }
  }

  /**
   * Get field metadata from schema for fixed-length parsing
   * @param segmentTag Segment tag
   * @param fieldIndex Field index within segment
   * @return Map containing field metadata: name, tag, mandatory, position, length, etc.
   */
  private def getFieldMetadata(segmentTag: String, fieldIndex: Int): Option[Map[String, Any]] = {
    context.fileFormatConfigModel.Segments.find(s => s.Segment_Tag.toString.trim == segmentTag) match {
      case Some(segment) =>
        if (segment.Fields != null && fieldIndex >= 0 && fieldIndex < segment.Fields.length) {
          val field = segment.Fields(fieldIndex)
          val maxLength = Option(field.Field_Maximum_Length).getOrElse(0).asInstanceOf[Int]
          
          // OPTIMIZATION: Calculate start position by summing lengths of all previous fields (only once per segment)
          // For fixed-length format: position = sum of all previous field lengths
          var startPos = 0
          for (i <- 0 until fieldIndex) {
            val prevFieldLength = Option(segment.Fields(i).Field_Maximum_Length).getOrElse(0).asInstanceOf[Int]
            startPos = startPos + prevFieldLength
          }
          
          Some(Map(
            "fieldIndex" -> fieldIndex,
            "fieldTag" -> field.Field_Tag.toString.trim,
            "fieldName" -> field.Field_Name.toString.trim,
            "isMandatory" -> field.Field_IsMandatory,
            "startPosition" -> startPos,
            "maxLength" -> maxLength,
            "isFiller" -> field.Field_IsFiller,
            "isDate" -> field.Field_IsDate,
            "dateFormat" -> field.Field_Date_Format.toString.trim
          ))
        } else None
      case None => None
    }
  }

  /**
   * Extract field value from segment line using fixed-length positions
   * @param segmentLine The raw segment line
   * @param startPos Starting position (1-based)
   * @param length Field length
   * @return Extracted field value (trimmed)
   */
  private def extractField(segmentLine: String, startPos: Int, length: Int): String = {
    if (startPos < 0 || length <= 0 || startPos >= segmentLine.length) return ""
    val endPos = Math.min(startPos + length, segmentLine.length)
    segmentLine.substring(startPos, endPos).trim
  }

  /**
   * Calculate hierarchical repeat ID for segments (same logic as PipeDelimitedParser)
   */
  private def calculateSegmentRepeatId(
    segment_tag: String,
    parentMap: Map[String, String],
    currentRepeatIdByTag: mutable.Map[String, String],
    siblingCount: mutable.Map[String, Int]
  ): String = {
    val parentTag = parentMap.getOrElse(segment_tag, "")
    val parentRepeatId = if (parentTag.isEmpty) "" else currentRepeatIdByTag.getOrElse(parentTag, "")
    val siblingKey = parentRepeatId + "|" + segment_tag
    
    val count = siblingCount.getOrElse(siblingKey, 0) + 1
    siblingCount(siblingKey) = count
    
    val repeatId = if (parentRepeatId.isEmpty) {
      count.toString
    } else {
      s"$parentRepeatId.$count"
    }
    
    currentRepeatIdByTag(segment_tag) = repeatId
    logger.debug(s"[calculateSegmentRepeatId] Calculated repeatId for $segment_tag: $repeatId")
    
    repeatId
  }

  /**
   * Evaluate record-level rules using pre-loaded RulesMapping
   * Rules are executed in priority order and return on first failure
   * Uses RuleEvaluationEngine with DSL-first strategy (DSL -> SQL -> PASS)
   * 
   * For fixed-length format, parses segment tags from fixed position (first 2 characters)
   * and calculates segment statistics for DSL evaluation
   * 
   * @param recordId The record ID being evaluated
   * @param rulesMapping The pre-loaded and indexed rules mapping containing all rule definitions
   * @param rawRecord The raw record string containing all segments
   * @return None if all rules pass, Some(RuleFailureResult) if any rule fails
   */
  private def evaluateRulesForRecord(recordId: String, rulesMapping: FixedLengthRulesMapping, rawRecord: String): Option[FixedLengthRuleFailureResult] = {
    import scala.util.control.Breaks._
    
    // Parse segments for evaluation
    val segmentLines = rawRecord.split("\r?\n").filter(_.trim.nonEmpty)

    // Actual segment sequence for the record (fixed-length tag is first 2 chars)
    val segment_sequence: Seq[String] = segmentLines.toSeq.map { segment_line =>
      if (segment_line.length >= 2) segment_line.substring(0, 2).trim else ""
    }

    // === CREATE SEGMENT VIEW FOR SQL RULE EVALUATION ===
    // This creates a temporary view `record_segments`(pos, tag) for modern, complex SQL-based ordering rules.
    try {
      val spark = context.spark
      import spark.implicits._

      val segmentsWithPos = segment_sequence.zipWithIndex.map { case (tag, idx) => (idx + 1, tag) }
      if (segmentsWithPos.nonEmpty) {
        val segmentsDF = segmentsWithPos.toDF("pos", "tag")
        segmentsDF.createOrReplaceTempView("record_segments")
        logger.debug(s"[evaluateRulesForRecord] [RECORD=$recordId] Created view 'record_segments' with ${segmentsWithPos.length} rows.")
      } else {
        logger.warn(s"[evaluateRulesForRecord] [RECORD=$recordId] No segments found, 'record_segments' view will be empty.")
        val schema = StructType(Seq(StructField("pos", IntegerType, false), StructField("tag", StringType, false)))
        spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], schema).createOrReplaceTempView("record_segments")
      }
    } catch {
      case ex: Throwable =>
        logger.warn(s"[evaluateRulesForRecord] [RECORD=$recordId] Failed to create 'record_segments' view: ${ex.getMessage}")
    }
    
    var failureResult: Option[FixedLengthRuleFailureResult] = None
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
              failureResult = Some(FixedLengthRuleFailureResult(
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
   * Evaluate segment-level rules for a given segment in fixed-length format
   * Segment-level rules validate segment structure and field count
   * 
   * For fixed-length, we validate:
   *   - Segment is present in correct position
   *   - Segment has minimum required fields
   *   - Field values conform to expected format/length
   * 
   * @param recordId Record ID for logging
   * @param segmentTag Segment tag (e.g., "BS", "RS", "CR", "GS")
   * @param segmentLine Raw segment line text
   * @param rulesMapping Pre-loaded rules mapping with segment-level rules
   * @param spark Spark session for rule evaluation
   * @return Option[RuleFailureResult] if any segment-level rule fails, None if all pass
   */
  private def evaluateRulesForSegment(recordId: String, segmentTag: String, segmentLine: String, 
                                     rulesMapping: FixedLengthRulesMapping, 
                                     spark: org.apache.spark.sql.SparkSession): Option[FixedLengthRuleFailureResult] = {
    import scala.util.control.Breaks._
    
    // Lookup segment-level rule IDs for this specific segment tag
    val segmentRuleIds = rulesMapping.segmentRulesBySegmentTag.getOrElse(segmentTag, Seq.empty)

    if (segmentRuleIds.isEmpty) {
      logger.debug(s"[RECORD=$recordId][SEGMENT=$segmentTag] No segment-level rules configured")
      return None
    }

    // Get segment metadata from schema (mandatory, max length, expected values, etc.)
    val segmentMetadataOpt = getSegmentMetadata(segmentTag)

    if (segmentMetadataOpt.isEmpty) {
      logger.warn(s"[RECORD=$recordId][SEGMENT=$segmentTag] Segment metadata not found in schema")
      return None
    }

    val segmentMetadata = segmentMetadataOpt.get
    val isMandatory = segmentMetadata.getOrElse("isMandatory", false).asInstanceOf[Boolean]
    val maxLength = segmentMetadata.getOrElse("maxLength", 0).asInstanceOf[Int]
    val segmentName = segmentMetadata.getOrElse("segmentName", "").asInstanceOf[String]

    logger.debug(s"[RECORD=$recordId][SEGMENT=$segmentTag] Segment metadata: name='$segmentName', isMandatory=$isMandatory, maxLength=$maxLength, actualLength=${segmentLine.length}")

    // Create a single-row DataFrame for segment validation and register it as a temporary view.
    // This view is used by the SQL rule engine to evaluate segment-level rules.
    val segmentValidationData = Seq(
      (recordId, segmentTag, segmentLine, isMandatory, maxLength, segmentLine.length)
    )
    val segmentValidationDf = spark.createDataFrame(segmentValidationData)
      .toDF("record_id", "segment_tag", "segment_line", "is_mandatory", "expected_max_length", "actual_length")
    segmentValidationDf.createOrReplaceTempView("df_segment")

    var failureResult: Option[FixedLengthRuleFailureResult] = None
    breakable {
      for (ruleId <- segmentRuleIds) {
        // Lookup rule definition from the pre-loaded rules map (O(1) lookup)
        rulesMapping.rulesById.get(ruleId) match {
          case Some(rule) =>
            // Explicitly check if this segment is listed in the rule's 'segment' array (if present)
            val allowedSegments: Seq[String] = rule.segment.map(_.map(_.toString.trim)).getOrElse(Seq.empty)
            if (allowedSegments.nonEmpty && !allowedSegments.contains(segmentTag)) {
              logger.debug(s"[RECORD=$recordId][SEGMENT=$segmentTag][RULE=$ruleId] Skipping rule because segmentTag not in rule.segment: ${allowedSegments.mkString(",")}")
              // Skip this rule for this segment
            } else {
              logger.debug(s"[RECORD=$recordId][SEGMENT=$segmentTag][RULE=$ruleId] Evaluating rule='${rule.rule_name.getOrElse("")}'")
              logger.debug(s"[RECORD=$recordId][SEGMENT=$segmentTag][RULE=$ruleId] Evaluating rule using SQL engine.")

                            // Evaluate rule using RuleEvaluationEngine
                            val passed = evaluationEngine.evaluateSegmentRule(rule, "df_segment")
              if (!passed) {
                failureResult = Some(FixedLengthRuleFailureResult(
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
   * Create a DataFrame for field-level validation for fixed-length format
   * This view will be used for evaluating field-level rules like R500001
   */
  private def createFieldValidationView(recordId: String, segmentTag: String, fieldIndex: Int, fieldValue: String, 
                                       fieldMetadata: Map[String, Any], spark: org.apache.spark.sql.SparkSession): Unit = {
    
    val isMandatory = fieldMetadata.getOrElse("isMandatory", false).asInstanceOf[Boolean]
    val maxLength = fieldMetadata.getOrElse("maxLength", 0).asInstanceOf[Int]
    val fieldName = fieldMetadata.getOrElse("fieldName", "").asInstanceOf[String]
    val fieldTag = fieldMetadata.getOrElse("fieldTag", "").asInstanceOf[String]
    
    logger.debug(s"[RECORD=$recordId][SEGMENT=$segmentTag][FIELD_INDEX=$fieldIndex] Schema field: name='$fieldName', tag='$fieldTag', isMandatory=$isMandatory, maxLength=$maxLength")
    logger.debug(s"[RECORD=$recordId][SEGMENT=$segmentTag][FIELD_INDEX=$fieldIndex] Actual field value: '$fieldValue' (length=${fieldValue.length}) - Comparison: LENGTH($fieldValue) <= $maxLength = ${fieldValue.length <= maxLength}")
    
    // Create a single-row DataFrame for field validation
    val fieldValidationData = Seq(
      (recordId, segmentTag, fieldIndex, fieldTag, fieldName, fieldValue, isMandatory, maxLength)
    )
    
    val fieldValidationDf = spark.createDataFrame(fieldValidationData)
      .toDF("record_id", "segment_tag", "field_index", "field_tag", "field_name", "field_value", "is_mandatory", "max_field_length")
    
    fieldValidationDf.createOrReplaceTempView("df_field_value")
    logger.debug(s"[RECORD=$recordId][SEGMENT=$segmentTag][FIELD_INDEX=$fieldIndex] Created df_field_value: is_mandatory=$isMandatory, max_field_length=$maxLength, field_value_length=${fieldValue.length}")
  }

  /**
   * Evaluate field-level rules for a specific field in fixed-length format
   * Fetches rules by key (segmentTag, fieldIndex) from fieldRulesBySegmentField
   * Returns failure result if any field-level rule fails
   * 
   * Supports both SQL and DSL rule evaluation for fields like:
   *   - R500001: Field value validation rules
   *   - Custom SQL rules for complex field validations
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
                                   fieldMetadata: Map[String, Any], rulesMapping: FixedLengthRulesMapping,
                                   segmentRepeatId: String,
                                   spark: org.apache.spark.sql.SparkSession): Option[FixedLengthRuleFailureResult] = {
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

    logger.debug(s"[RECORD=$recordId][SEGMENT=$segmentTag][FIELD=$fieldName] Evaluating ${fieldRuleIds.size} field-level rules")

    // Create field validation view for this field
    createFieldValidationView(recordId, segmentTag, fieldIndex, fieldValue, fieldMetadata, spark)

    var failureResult: Option[FixedLengthRuleFailureResult] = None
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
              logger.debug(s"[RECORD=$recordId][SEGMENT=$segmentTag][FIELD=$fieldName][RULE=$ruleId] Evaluating rule='${rule.rule_name.getOrElse("")}'")
              logger.debug(s"[RECORD=$recordId][SEGMENT=$segmentTag][FIELD=$fieldName][RULE=$ruleId] Evaluating rule using SQL engine.")

              // Skip field-level rule evaluation if field is not mandatory and rule requires mandatory check
              val ruleType = rule.rule_type.getOrElse("")
              if (ruleType == "MANDATORY" && !isMandatory) {
                logger.debug(s"[RECORD=$recordId][SEGMENT=$segmentTag][FIELD=$fieldName][RULE=$ruleId] Skipping mandatory-type rule for non-mandatory field")
                // Continue to next rule
              } else {
                // Evaluate rule using RuleEvaluationEngine
                val passed = evaluationEngine.evaluateFieldRule(rule, "df_field_value")

                if (!passed) {
                  failureResult = Some(FixedLengthRuleFailureResult(
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
   * Parse fixed-length file based on provided configuration
   * 
   * Flow:
   * 1. Build rules mapping (record + segment + field level rules)
   * 2. For each record:
   *    a. Evaluate record-level rules (R300001 - segment order check)
   *    b. For each segment in record:
   *       - Extract fields using fixed-length positions
   *       - Evaluate field-level rules (R500001 - field length check)
   *    c. Flatten segment/field data and output as rows
   * 
   * @param sourceDF Source DataFrame to parse
   * @return Parsed DataFrame with segment, field, and error information
   */
  def parseFile(sourceDF: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    
    val spark = context.spark
    val fileLoadId = context.fileLoadId
    val rulePath = context.fileConfigModel.ruleLocation
    val nowTime = CommercialUtility.nowFormatted(CommercialConstants.DATETIMEFORMAT)
    
    if (rulePath == null || rulePath.trim.isEmpty) {
      logger.error("[parseFile] Rule location not configured in fileConfigModel")
      return sourceDF
    }
    
    logger.info(s"[parseFile] Starting FixedLength file parsing. FileLoadID=$fileLoadId, RulePath=$rulePath")
    
    // [STEP 1] Process header/footer segments early (HD/TS segments are independent of record processing)
    val df2HT = processHeaderFooterSegments(sourceDF, spark)
    logger.info(s"[STEP 1] Header/footer segments processed")
    
    // [STEP 2] Build rules mapping for record and field level rules
    val rulesMapping = buildRulesMapping(rulePath)
    logger.info(s"[parseFile] Built RulesMapping: ${rulesMapping.recordRuleIds.size} record rules, ${rulesMapping.fieldRulesBySegmentField.size} field-level rules")
    
    // [STEP 3] Collect records and process them locally (avoiding RDD serialization issues)
    logger.info(s"[parseFile] Converting DataFrame to local collection for processing (avoiding RDD serialization issues)")
    
    val rows = sourceDF.collect()
    val parentMap = context.fileFormatConfigModel.Segments
      .filter(s => s.Segment_Parent != null && s.Segment_Parent.nonEmpty)
      .map(s => s.Segment_Tag.toString.trim -> s.Segment_Parent.toString.trim)
      .toMap
    
    // Process each row locally (driver-side processing)
    val outputRowList = scala.collection.mutable.ListBuffer[FixedLengthSegmentOutputRow]()
    
    for (row <- rows) {
      val recordId = row.getAs[String]("RecordID")
      val batchId = row.getAs[Int]("BatchID")
      val rawRecord = row.getAs[String]("RawRecord")
      
      logger.info(s"[parseFile] Processing record: $recordId")

      // Create a single-row df_raw_record view for this record so SQL rules / SQL expressions can run via SqlRuleEvaluator.
      // (The per-record view is enriched with derived columns inside evaluateRulesForRecord.)
      try {
        val one = spark.createDataFrame(
          java.util.Arrays.asList(row),
          sourceDF.schema
        )
        one.createOrReplaceTempView("df_raw_record")
      } catch {
        case ex: Throwable =>
          logger.warn(s"[parseFile] [RECORD=$recordId] Failed to create df_raw_record view: ${ex.getMessage}")
      }
      
      // Evaluate record-level rules
      val recordRuleFailure = evaluateRulesForRecord(recordId, rulesMapping, rawRecord)
      
      if (recordRuleFailure.isDefined) {
        val failure = recordRuleFailure.get
        logger.warn(s"[parseFile] [RECORD=$recordId] Record-level rule failed: ${failure.ruleId}, errorCode=${failure.errorCode}")
        
        // Output single row with record-level rule failure
        outputRowList += FixedLengthSegmentOutputRow(
          fileLoadId = fileLoadId,
          batchId = batchId,
          recordId = recordId,
          recordInsertionTime = nowTime,
          segment_tag = "",
          field_tag = "",
          field_value = "",
          segment_repeat_id = null,
          error_code = failure.errorCode,
          error_message = failure.errorMessage,
          rule_id = failure.ruleId,
          metric_code = failure.metricCode,
          reject_type = failure.ruleAction + CommercialConstants.UNDERSCORESIGN + failure.ruleLevel
        )
      } else {
        // Record-level rules passed, process segments and fields
        val segmentLines = rawRecord.split("\r?\n").filter(_.trim.nonEmpty)
        val currentRepeatIdByTag = mutable.Map[String, String]()
        val siblingCount = mutable.Map[String, Int]()
        
        segmentLines.foreach { segmentLine =>
          if (segmentLine.nonEmpty) {
            val segmentTag = if (segmentLine.length >= 2) segmentLine.substring(0, 2).trim else ""
            logger.debug(s"[parseFile] [RECORD=$recordId][SEGMENT=$segmentTag] Processing segment line (length=${segmentLine.length})")
            
            if (segmentTag.nonEmpty) {
              // Calculate repeat ID
              val segmentRepeatId = calculateSegmentRepeatId(segmentTag, parentMap, currentRepeatIdByTag, siblingCount)
              
              // Get segment metadata
              val segmentOpt = context.fileFormatConfigModel.Segments.find(s => s.Segment_Tag.toString.trim == segmentTag)
              
              if (segmentOpt.isDefined) {
                val segment = segmentOpt.get
                logger.debug(s"[parseFile] [RECORD=$recordId][SEGMENT=$segmentTag] Segment metadata: isMandatory=${segment.Segment_IsMandatory}, maxLength=${segment.Segment_Maximum_Length}")
                
                // Evaluate segment-level rules for this segment
                val segmentRuleFailure = evaluateRulesForSegment(recordId, segmentTag, segmentLine, rulesMapping, spark)
                
                if (segmentRuleFailure.isDefined) {
                  val failure = segmentRuleFailure.get
                  logger.warn(s"[parseFile] [RECORD=$recordId][SEGMENT=$segmentTag][RULE=${failure.ruleId}] FAILED - errorCode='${failure.errorCode}'")
                  
                  // Output row with segment-level rule failure
                  outputRowList += FixedLengthSegmentOutputRow(
                    fileLoadId = fileLoadId,
                    batchId = batchId,
                    recordId = recordId,
                    recordInsertionTime = nowTime,
                    segment_tag = segmentTag,
                    field_tag = "",
                    field_value = "",
                    segment_repeat_id = segmentRepeatId,
                    error_code = failure.errorCode,
                    error_message = failure.errorMessage,
                    rule_id = failure.ruleId,
                    metric_code = failure.metricCode,
                    reject_type = failure.ruleAction + CommercialConstants.UNDERSCORESIGN + failure.ruleLevel
                  )
                } else {
                  // Segment-level rules passed, now extract all fields from this segment
                  if (segment.Fields != null && segment.Fields.nonEmpty) {
                  // OPTIMIZATION: Build field metadata map once per segment instead of per field
                  val fieldMetadataMap = scala.collection.mutable.Map[Int, Map[String, Any]]()
                  for (fieldIdx <- 0 until segment.Fields.length) {
                    getFieldMetadata(segmentTag, fieldIdx).foreach { metadata =>
                      fieldMetadataMap(fieldIdx) = metadata
                    }
                  }
                  
                  var fieldFailure = false
                  breakable {
                    for (fieldIdx <- 0 until segment.Fields.length) {
                      fieldMetadataMap.get(fieldIdx) match {
                        case Some(metadata) =>
                          val startPos = metadata("start_position").asInstanceOf[Int]
                          val maxLength = metadata("max_field_length").asInstanceOf[Int]
                          val isFiller = metadata("is_filler").asInstanceOf[Boolean]
                          val fieldValue = extractField(segmentLine, startPos, maxLength)
                          val fieldTag = metadata("field_tag").asInstanceOf[String]
                          
                          logger.debug(s"[parseFile] [RECORD=$recordId][SEGMENT=$segmentTag][FIELD_INDEX=$fieldIdx] Extracted field_tag=$fieldTag, value='$fieldValue' (length=${fieldValue.length})")
                          
                          // Skip field-level rule evaluation for filler fields
                          if (!isFiller) {
                            // Evaluate field-level rules
                            val fieldRuleFailure = evaluateRulesForField(recordId, segmentTag, fieldIdx, fieldValue, metadata, rulesMapping, segmentRepeatId, spark)
                            
                            if (fieldRuleFailure.isDefined) {
                              val failure = fieldRuleFailure.get
                              logger.warn(s"[parseFile] [RECORD=$recordId][SEGMENT=$segmentTag][FIELD_INDEX=$fieldIdx][RULE=${failure.ruleId}] FAILED - errorCode='${failure.errorCode}'")
                              
                              // Output row with field-level rule failure
                              outputRowList += FixedLengthSegmentOutputRow(
                                fileLoadId = fileLoadId,
                                batchId = batchId,
                                recordId = recordId,
                                recordInsertionTime = nowTime,
                                segment_tag = segmentTag,
                                field_tag = fieldTag,
                                field_value = fieldValue,
                                segment_repeat_id = segmentRepeatId,
                                error_code = failure.errorCode,
                                error_message = failure.errorMessage,
                                rule_id = failure.ruleId,
                                metric_code = failure.metricCode,
                                reject_type = failure.ruleAction + CommercialConstants.UNDERSCORESIGN + failure.ruleLevel
                              )
                              fieldFailure = true
                              break()
                            }
                          }
                          
                          // Output row with field data (success case)
                          outputRowList += FixedLengthSegmentOutputRow(
                            fileLoadId = fileLoadId,
                            batchId = batchId,
                            recordId = recordId,
                            recordInsertionTime = nowTime,
                            segment_tag = segmentTag,
                            field_tag = fieldTag,
                            field_value = fieldValue,
                            segment_repeat_id = segmentRepeatId,
                            error_code = null,
                            error_message = null,
                            rule_id = null,
                            metric_code = null,
                            reject_type = null
                          )
                        
                        case None =>
                          logger.debug(s"[parseFile] [RECORD=$recordId][SEGMENT=$segmentTag][FIELD_INDEX=$fieldIdx] No metadata found for field")
                      }
                    }
                  }
                  
                  // If field-level rule failed, skip remaining fields for this segment
                  if (fieldFailure) {
                    logger.info(s"[parseFile] [RECORD=$recordId][SEGMENT=$segmentTag] Segment-level rule or field-level rule failed; skipping remaining fields")
                  }
                }
                }
              } else {
                logger.warn(s"[parseFile] [RECORD=$recordId][SEGMENT=$segmentTag] Segment metadata not found in schema")
              }
            }
          }
        }
      }
      
      logger.info(s"[parseFile] [RECORD=$recordId] Generated ${outputRowList.size} output rows")
    }
    
    // Create DataFrame from collected output rows
    val schema = buildSegmentOutputSchema()
    val resultDF = if (outputRowList.nonEmpty) {
      spark.createDataFrame(
        spark.sparkContext.parallelize(
          outputRowList.map(row => toSparkRow(row))
        ),
        schema
      )
    } else {
      spark.createDataFrame(Seq.empty[Row].asJava, schema)
    }

    // [STEP 4] Add field tag columns and Type_of_Line to record segments
    val with49 = addFieldTagColumns(resultDF)
    val df2Rec = with49
      .withColumn("Type_of_Line", lit("Record"))
      .withColumn("CreatedAt", lit(nowTime).cast(StringType))
    logger.info(s"Record segment count: ${df2Rec.count()}")

    // [STEP 5] Union header/footer segments (processed in STEP 1) with record segments
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

    val df2Final = df2HT.select(selectedCols: _*).unionByName(df2Rec.select(selectedCols: _*))
    logger.info(s"Final DF2 (after union with header/footer): ${df2Final.count()} rows")

    CommercialUtility.writeDataFrame(context, context.parserConfigModel.dataFrames.dataframe2, df2Final)
    
    // --- CLEANUP TEMPORARY VIEWS AND CACHED DATA ---
    CommercialUtility.cleanupTemporaryViews(spark, List("df_raw_record", "df_field_value", "df_segment"))
    
    df2Final
  }
  
}
