package com.experian.ind.prime.ingest.core.parser.commercial.engine

import com.experian.ind.prime.ingest.core.Util.parser.commercial.DualLogger
import com.experian.ind.prime.ingest.core.parser.commercial.rules.{RulesRepository, SqlRuleEvaluator}
import com.experian.ind.prime.ingest.core.parser.commercial.util.{CommercialConstants, CommercialUtility}
import com.experian.ind.prime.ingest.core.shared_models.PipelineContext
import com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial.{DF3DtlRejSumm, DF4RecRejSumm, RuleDefinition, Segment}
import org.apache.spark.sql.functions.{col, lit, lower}
import org.apache.spark.sql.{DataFrame, Row}

import scala.reflect.runtime.universe._
import scala.util.control.Breaks.{break, breakable}

final class FixedLengthHTParser(context: PipelineContext) {
  private lazy val logger = DualLogger(getClass)
  private val spark = context.spark
  import spark.implicits._

  private val repository = new RulesRepository(context.spark)
  private val evaluator = new SqlRuleEvaluator(context.spark)

  /**
   * Safely cache a table if not already cached
   */
  private def cacheIfNotCached(spark: org.apache.spark.sql.SparkSession, viewName: String): Unit = {
    if (!spark.catalog.isCached(viewName)) {
      spark.catalog.cacheTable(viewName)
    }
  }
  // Using typed case classes DF3DtlRejSumm and DF4RecRejSumm instead of manual StructType schemas

  /**
   * Dynamically fetch constraint keys from Segment case class using reflection
   * No hardcoded field names - discovers field names by pattern matching
   * This makes the code fully configuration-driven based on actual case class structure
   */
  private def fetchSchemaConstraintKeys(context: PipelineContext): Map[String, String] = {
    try {
      // Use Scala reflection to get all field names from Segment case class
      val segmentType = typeOf[Segment]
      val fieldNames = segmentType.members
        .collect { case m: TermSymbol if m.isVal => m.name.toString }
        .toList
      
      // Dynamically discover constraint fields by pattern matching (no hardcoding)
      val mandatoryField = fieldNames.find(_.toLowerCase.contains("ismandatory"))
      val lengthField = fieldNames.find(_.toLowerCase.contains("maximum_length"))
      val occurrenceField = fieldNames.find(_.toLowerCase.contains("occurrence"))
      val delimiterField = fieldNames.find(_.toLowerCase.contains("delimiter_count"))
      
      Map(
        "mandatory" -> mandatoryField.getOrElse(CommercialConstants.SEGEMENT_MANDATORY),
        "length" -> lengthField.getOrElse(CommercialConstants.SEGMENT_LENGTH),
        "occurrence" -> occurrenceField.getOrElse(CommercialConstants.SEGMENT_OCCURRENCE),
        "delimiter_count" -> delimiterField.getOrElse(CommercialConstants.SEGEMENT_DELIMITER_COUNT)
      )
    } catch {
      case ex: Exception =>
        logger.warn(s"[SCHEMA] Failed to dynamically fetch constraint keys: ${ex.getMessage}; using fallback field names")
        // Fallback to standard field names if reflection fails
        Map(
          "mandatory" -> CommercialConstants.SEGEMENT_MANDATORY,
          "length" -> CommercialConstants.SEGMENT_LENGTH,
          "occurrence" -> CommercialConstants.SEGMENT_OCCURRENCE,
          "delimiter_count" -> CommercialConstants.SEGEMENT_DELIMITER_COUNT
        )
    }
  }

  /**
   * Helper method to create segment view (constraints or fields) based on format
   * Eliminates duplicate code for header/footer view creation
   * Uses schema keys fetched from configuration instead of hardcoding
   */
  private def createSegmentView(
    context: PipelineContext,
    segmentTag: String,
    typeOfLine: String,
    viewNameSuffix: String,
    isConstraint: Boolean = false
  ): String = {
    val viewName = s"${Option(typeOfLine).getOrElse(if (isConstraint) "unknown" else "").toLowerCase}${viewNameSuffix}"
    
    if (isConstraint) {
      // Fetch schema constraint keys dynamically instead of hardcoding
      val constraintKeys = fetchSchemaConstraintKeys(context)
      val keys = Seq(
        constraintKeys("mandatory"),
        constraintKeys("length"),
        constraintKeys("occurrence"),
        constraintKeys("delimiter_count")
      )
      
      CommercialUtility.createSegmentConstraintsView(
        context,
        segmentTag = segmentTag,
        keys = keys,
        aliases = Seq("segment_is_mandatory", "segment_max_length", "segment_occurrence", "segment_delimiter_count"),
        viewName = viewName
      )
    } else {
      // For fixed-length format, use generic field view
      CommercialUtility.createSegmentFieldsView(
        context,
        segmentTag = segmentTag,
        viewName = viewName
      )
    }
    viewName
  }

  def parseFile(
    sourceDF: DataFrame
  ): List[DataFrame] = {
    val metadata = context.metadata
    val fileLoadId = context.fileLoadId
    val parserConfigModel = context.parserConfigModel
    val fileFormatConfigModel = context.fileFormatConfigModel
    val fileConfigModel = context.fileConfigModel
    val inputFileName = metadata.fileName
    val inputFilePath = metadata.inputPath
    val outputPath = parserConfigModel.paths.outputPath

    logger.info(s"[FixedLengthHTParser] Starting header-trailer rule evaluation for fixed-length format")
    logger.info(s"[FixedLengthHTParser] fileLoadId=${fileLoadId}, file='${inputFileName}', inputPath='${inputFilePath}'")

    // Extract rule IDs from schema and rule file path from configuration
    val ruleIds = Option(fileFormatConfigModel.fileStructure.ruleId).map(_.toSeq).getOrElse(Seq.empty)
    if (ruleIds.isEmpty) {
      logger.info("[FixedLengthHTParser] No ruleIds configured in schema fileStructure; returning empty DF3/DF4")
      return List.empty
    }

    val rulesPath = fileConfigModel.ruleLocation
    if (rulesPath == null || rulesPath.trim.isEmpty) {
      logger.error("[FixedLengthHTParser] ruleLocation not found in file-config.json; cannot evaluate file-level rules")
      return List.empty
    }

    // 1) Prepare df_raw view for SQL rules
    logger.info(s"[FixedLengthHTParser] Preparing df_raw view from sourceDF for SQL rule evaluation")
    sourceDF.createOrReplaceTempView("df_raw")
    cacheIfNotCached(context.spark, "df_raw")

    // Create header views (constraints + fields) using helper method
    val startSegTag = context.fileFormatConfigModel.fileStructure.startSegment
    val startTypeOfLine = CommercialUtility.fetchSegmentTypeofLine(context, CommercialConstants.HEADER)
    logger.debug(s"[FixedLengthHTParser] Creating header views: segmentTag='${startSegTag}', typeOfLine='${startTypeOfLine}'")
    
    val headerViewDynamic = createSegmentView(
      context,
      segmentTag = startSegTag,
      typeOfLine = startTypeOfLine,
      viewNameSuffix = "_constraints",
      isConstraint = true
    )
    val headerFieldsView = createSegmentView(
      context,
      segmentTag = startSegTag,
      typeOfLine = startTypeOfLine,
      viewNameSuffix = "_fields",
      isConstraint = false
    )
    
    // Create footer views (constraints + fields) using helper method
    val endSegTag = context.fileFormatConfigModel.fileStructure.endSegment
    val endTypeOfLine = CommercialUtility.fetchSegmentTypeofLine(context, CommercialConstants.FOOTER)
    logger.debug(s"[FixedLengthHTParser] Creating footer views: segmentTag='${endSegTag}', typeOfLine='${endTypeOfLine}'")
    
    val footerViewDynamic = createSegmentView(
      context,
      segmentTag = endSegTag,
      typeOfLine = endTypeOfLine,
      viewNameSuffix = "_constraints",
      isConstraint = true
    )
    val footerFieldsView = createSegmentView(
      context,
      segmentTag = endSegTag,
      typeOfLine = endTypeOfLine,
      viewNameSuffix = "_fields",
      isConstraint = false
    )
    
    // 2) Validate and load rules (ACTIVE + SQL) by IDs in priority order
    // Robust null/empty checks with early return
    val rulesPathTrimmed = Option(rulesPath).map(_.trim).filter(_.nonEmpty)
    val ruleIdsTrimmed = Option(ruleIds).getOrElse(Seq.empty).filter(_.trim.nonEmpty)
    
    if (rulesPathTrimmed.isEmpty || ruleIdsTrimmed.isEmpty) {
      logger.warn("[FixedLengthHTParser] rulesPath or ruleIds is null/empty; skipping rule evaluation and returning early")
      CommercialUtility.cleanupTemporaryViews(context.spark, List("df_raw", headerViewDynamic, headerFieldsView, footerViewDynamic, footerFieldsView))
      return List.empty[DataFrame]
    }
    
    logger.info(s"[FixedLengthHTParser] Loading all active rules (SQL + DSL) from path='${rulesPathTrimmed.get}' for ruleIds=[${ruleIdsTrimmed.mkString(",")}]")
    val rulesDS = repository.loadActiveRules(rulesPathTrimmed.get, ruleIdsTrimmed)
    
    // 3) Evaluate and collect failures
    val df3Rows = scala.collection.mutable.ArrayBuffer.empty[DF3DtlRejSumm]
    val df4Rows = scala.collection.mutable.ArrayBuffer.empty[DF4RecRejSumm]
    var rejectionCounter = 0  // Counter to track number of rule failures

    logger.info(s"[FixedLengthHTParser] Starting rule evaluation loop")

    // Resolve identifiers from sourceDF in order: Header -> Footer -> first Record
    val typeCol = sourceDF.columns.find(_.equalsIgnoreCase(CommercialConstants.TYPEOFLINE)).getOrElse("Type_of_Line")

    // Helper to resolve the actual column name in a case-insensitive way with fallbacks
    def resolveColumnName(df: DataFrame, candidates: Seq[String]): Option[String] = {
      val lowerToActual = df.columns.map(c => c.toLowerCase -> c).toMap
      candidates.collectFirst { case name => lowerToActual.get(name.toLowerCase) }.flatten
    }

    def firstRowByType(t: String): Option[Row] = {
      val d = sourceDF.filter(lower(col(typeCol)) === t.toLowerCase).limit(1)
      val rows = d.head(1)
      if (rows.isEmpty) None else Some(rows.head)
    }

    // Fetch a value from a row using an already-resolved actual column name
    def getVal(rowOpt: Option[Row], actualColName: String): Option[String] =
      rowOpt.flatMap(r => Option(r.getAs[Any](actualColName)).map(_.toString))

    // Resolve typeof-line names from schema via common resolver
    val headerName = CommercialUtility.fetchSegmentTypeofLine(context, CommercialConstants.HEADER)
    val headerRow = firstRowByType(headerName)
    logger.debug(s"[FixedLengthHTParser] Header row resolved: ${headerRow.isDefined}")

    val footerName = CommercialUtility.fetchSegmentTypeofLine(context, CommercialConstants.FOOTER)
    val footerRow = firstRowByType(footerName)
    logger.debug(s"[FixedLengthHTParser] Footer row resolved: ${footerRow.isDefined}")

    val recordName = CommercialUtility.fetchSegmentTypeofLine(context, CommercialConstants.RECORD)
    val recordRow = firstRowByType(recordName)
    logger.debug(s"[FixedLengthHTParser] Record row resolved: ${recordRow.isDefined}")

    // Determine actual column names for identifiers from sourceDF (with common fallbacks)
    val fileLoadIdCol = resolveColumnName(sourceDF, Seq(CommercialConstants.FILELOADID)).getOrElse("FileLoadID")
    val batchIdCol    = resolveColumnName(sourceDF, Seq(CommercialConstants.BATCHID)).getOrElse("BatchID")
    val recordIdCol   = resolveColumnName(sourceDF, Seq(CommercialConstants.RECORDID)).getOrElse("RecordID")

    logger.debug(s"[FixedLengthHTParser] Resolved columns: fileLoadIdCol='${fileLoadIdCol}', batchIdCol='${batchIdCol}', recordIdCol='${recordIdCol}'")

    // First record row with same FileLoadID as current file load (if available)
    def recordRowWithSameFileLoadId: Option[Row] = {
      try {
        val d = sourceDF
          .filter(lower(col(typeCol)) === recordName.toLowerCase)
          .filter(col(fileLoadIdCol) === lit(fileLoadId))
          .limit(1)
        val rows = d.head(1)
        if (rows.isEmpty) None else Some(rows.head)
      } catch {
        case _: Throwable => None
      }
    }

    val nowStr = CommercialUtility.nowFormatted(CommercialConstants.DATETIMEFORMAT)

    // Collect rules safely with null/empty checks
    val rulesList: Seq[RuleDefinition] = if (rulesDS != null) {
      val rules = rulesDS.collect().toSeq
      logger.info(s"[FixedLengthHTParser] Loaded ${rules.size} active rules (SQL + DSL) from '${rulesPathTrimmed.get}'")
      rules
    } else {
      logger.warn("[FixedLengthHTParser] rulesDS is null; no rules to evaluate")
      Seq.empty[RuleDefinition]
    }
    
    // Early exit if no rules loaded
    if (rulesList.isEmpty) {
      logger.info("[FixedLengthHTParser] No rules loaded; skipping evaluation")
      CommercialUtility.cleanupTemporaryViews(context.spark, List("df_raw", headerViewDynamic, headerFieldsView, footerViewDynamic, footerFieldsView))
      return List.empty[DataFrame]
    }

    breakable {
      // Segment tags from schema to identify header/footer related rules
      val headerTag = context.fileFormatConfigModel.fileStructure.startSegment
      val footerTag = context.fileFormatConfigModel.fileStructure.endSegment

      def extractIds(rowOpt: Option[Row]): (Long, Int, String) = {
        val fid = getVal(rowOpt, fileLoadIdCol).flatMap(v => scala.util.Try(v.toLong).toOption).getOrElse(fileLoadId)
        val bid = getVal(rowOpt, batchIdCol).flatMap(v => scala.util.Try(v.toInt).toOption).getOrElse(0)
        val rid = getVal(rowOpt, recordIdCol).getOrElse("")
        (fid, bid, rid)
      }

      def deriveIdsForRule(rule: RuleDefinition): (Long, Int, String) = {
        val segs: Seq[String] = rule.segment.getOrElse(Seq.empty[String])
        
        if (segs.contains(headerTag)) {
          logger.debug(s"[FixedLengthHTParser] Rule '${rule.rule_id}' applies to header; deriving IDs from header row")
          extractIds(headerRow)
        } else if (segs.contains(footerTag)) {
          logger.debug(s"[FixedLengthHTParser] Rule '${rule.rule_id}' applies to footer; deriving IDs from footer row")
          extractIds(footerRow)
        } else {
          logger.debug(s"[FixedLengthHTParser] Rule '${rule.rule_id}' applies to records; deriving IDs from first record row")
          extractIds(recordRowWithSameFileLoadId.orElse(recordRow))
        }
      }

      for (rule <- rulesList) {
        val ruleName = rule.rule_name.getOrElse("UNNAMED")
        val ruleSeverity = rule.severity.getOrElse("N/A")
        val ruleAction = rule.rule_action.getOrElse("UNKNOWN")
        val ruleLevel = rule.rule_level.getOrElse("UNKNOWN")
        
        logger.debug(s"[FixedLengthHTParser] [RULE][${rule.rule_id}] name='${ruleName}', level='${ruleLevel}', severity='${ruleSeverity}'")
        
        // Derive IDs for DF3/DF4 based on rule segment applicability
        val (ridFileLoadId, ridBatchId, ridRecordId) = deriveIdsForRule(rule)
        
        // Execute the SQL rule using the evaluator
        val sqlExpression = rule.SQL_expression.getOrElse("")
        logger.debug(s"[FixedLengthHTParser] [RULE][${rule.rule_id}] Executing SQL condition against df_raw. sqlExpression='${sqlExpression.take(100)}'")
        
        try {
          val passed = evaluator.evaluateOnRaw("df_raw", rule)
          if (!passed) {
            logger.warn(s"[FixedLengthHTParser] [RULES][FAIL] ruleId='${rule.rule_id}', errorCode='${rule.error_code.getOrElse("")}', severity='${ruleSeverity}', action='${ruleAction}'")
            rejectionCounter += 1
            val (ridFileLoadId, ridBatchId, ridRecordId) = deriveIdsForRule(rule)
            
            df3Rows += DF3DtlRejSumm(
              FileLoadID = ridFileLoadId,
              BatchID = ridBatchId,
              RecordID = ridRecordId,
              Segment_Tag = rule.segment.flatMap(_.headOption).orNull,
              Segment_Repeat_Id = null,
              Field_Tag = CommercialUtility.normalizeEmptyArrayString(rule.field_tag),
              Field_Value = null,
              Error_Stage = rule.rule_stage.getOrElse(CommercialConstants.ERROR_STAGE),
              Severity = rule.severity.getOrElse(CommercialConstants.SEVERITY),
              Error_Code = rule.error_code.getOrElse(null),
              Error_Message = rule.error_message.getOrElse(null),
              Rule_Id = rule.rule_id,
              Metric_Code = rule.metric_code.getOrElse(null),
              Reject_Type = (rule.rule_action, rule.rule_level) match {
                case (Some(a), Some(l)) => s"${a}_${l}"
                case (Some(a), None) => a
                case (None, Some(l)) => l
                case _ => null
              },
              Created_At = nowStr
            )

            df4Rows += DF4RecRejSumm(
              FileLoadID = ridFileLoadId,
              BatchID = ridBatchId,
              RecordID = ridRecordId,
              IsFile_Reject = if (rule.rule_level.exists(_.equalsIgnoreCase(CommercialConstants.RULE_LEVEL_FILE)) && rule.rule_action.exists(_.equalsIgnoreCase(CommercialConstants.RULEACTION_REJECT))) CommercialConstants.TRUE else CommercialConstants.FALSE,
              IsRecord_Reject = if (rule.rule_level.exists(_.equalsIgnoreCase(CommercialConstants.RULE_LEVEL_RECORD)) && rule.rule_action.exists(_.equalsIgnoreCase(CommercialConstants.RULEACTION_REJECT))) CommercialConstants.TRUE else CommercialConstants.FALSE,
              IsSegment_Reject = if (rule.rule_level.exists(_.equalsIgnoreCase(CommercialConstants.RULE_LEVEL_SEGEMENT)) && rule.rule_action.exists(_.equalsIgnoreCase(CommercialConstants.RULEACTION_REJECT))) CommercialConstants.TRUE else CommercialConstants.FALSE,
              IsField_Reject = if (rule.rule_level.exists(_.equalsIgnoreCase(CommercialConstants.RULE_LEVEL_FIELD)) && rule.rule_action.exists(_.equalsIgnoreCase(CommercialConstants.RULEACTION_REJECT))) CommercialConstants.TRUE else CommercialConstants.FALSE
            )
          } else {
            logger.debug(s"[FixedLengthHTParser] [RULE][${rule.rule_id}] PASSED")
          }
        } catch {
          case ex: Exception =>
            logger.error(s"[FixedLengthHTParser] [RULE][${rule.rule_id}] Exception during rule evaluation: ${ex.getMessage}", ex)
            rejectionCounter += 1
            val (ridFileLoadId, ridBatchId, ridRecordId) = deriveIdsForRule(rule)
            
            df3Rows += DF3DtlRejSumm(
              FileLoadID = ridFileLoadId,
              BatchID = ridBatchId,
              RecordID = ridRecordId,
              Segment_Tag = rule.segment.flatMap(_.headOption).orNull,
              Segment_Repeat_Id = null,
              Field_Tag = CommercialUtility.normalizeEmptyArrayString(rule.field_tag),
              Field_Value = null,
              Error_Stage = rule.rule_stage.getOrElse(CommercialConstants.ERROR_STAGE),
              Severity = "ERROR",
              Error_Code = "RULE_EXEC_ERROR",
              Error_Message = s"Rule execution error: ${ex.getMessage}",
              Rule_Id = rule.rule_id,
              Metric_Code = rule.metric_code.getOrElse(null),
              Reject_Type = (rule.rule_action, rule.rule_level) match {
                case (Some(a), Some(l)) => s"${a}_${l}"
                case (Some(a), None) => a
                case (None, Some(l)) => l
                case _ => null
              },
              Created_At = nowStr
            )

            df4Rows += DF4RecRejSumm(
              FileLoadID = ridFileLoadId,
              BatchID = ridBatchId,
              RecordID = ridRecordId,
              IsFile_Reject = CommercialConstants.TRUE,
              IsRecord_Reject = CommercialConstants.FALSE,
              IsSegment_Reject = CommercialConstants.FALSE,
              IsField_Reject = CommercialConstants.FALSE
            )

            // Continue to next rule even if current rule evaluation fails
            logger.warn(s"[FixedLengthHTParser] [RULES] Continuing to next rule after exception in ruleId='${rule.rule_id}'")
        }
      }
    }

    logger.info(s"[FixedLengthHTParser] Rule evaluation completed. Total rejections: ${rejectionCounter}")

    if (rejectionCounter > 0) {
      // Process rejection DataFrames (DF3 and DF4)
      logger.info(s"[FixedLengthHTParser] Processing rejection DataFrames (DF3, DF4)")
      val df3 = spark.createDataset(df3Rows.toSeq).toDF()
      val df4 = spark.createDataset(df4Rows.toSeq).toDF()

      val hasDf3Rows = df3.head(1).nonEmpty
      val hasDf4Rows = df4.head(1).nonEmpty
      
      // Cache filename without extension (reuse for both DF3 and DF4)
      val fileNameWithoutExt = Option(inputFileName)
        .filter(_.trim.nonEmpty)
        .map(_.replaceAll("\\.[^.]*$", ""))
        .getOrElse("output")
      
      if (hasDf3Rows) {
        logger.info(s"[FixedLengthHTParser] DF3 detail rejects to write: ${df3Rows.size}")
        CommercialUtility.writeDataFrame(context, context.parserConfigModel.dataFrames.dataframe3, df3)
        logger.info(s"[FixedLengthHTParser] DF3 written to '${outputPath}' with fileLoadId=$fileLoadId")
      }
      if (hasDf4Rows) {
        logger.info(s"[FixedLengthHTParser] DF4 record rejects to write: ${df4Rows.size}")
        CommercialUtility.writeDataFrame(context, context.parserConfigModel.dataFrames.dataframe4, df4)
        logger.info(s"[FixedLengthHTParser] DF4 written to '${outputPath}' with fileLoadId=$fileLoadId")
      }
      
      // --- CLEANUP TEMPORARY VIEWS AND CACHED DATA ---
      logger.debug(s"[FixedLengthHTParser] Cleaning up temporary views and cached data")
      CommercialUtility.cleanupTemporaryViews(context.spark, List("df_raw", headerViewDynamic, headerFieldsView, footerViewDynamic, footerFieldsView))
      
      logger.info(s"[FixedLengthHTParser] Returning ${List(df3, df4).size} rejection DataFrames (DF3, DF4)")
      List(df3, df4)
    } else {
      // No rejections - return empty to signal DF2 fallback
      logger.info(s"[FixedLengthHTParser] No rejections found. Signaling DF2 parsing fallback")
      
      // --- CLEANUP TEMPORARY VIEWS AND CACHED DATA ---
      logger.debug(s"[FixedLengthHTParser] Cleaning up temporary views and cached data")
      CommercialUtility.cleanupTemporaryViews(context.spark, List("df_raw", headerViewDynamic, headerFieldsView, footerViewDynamic, footerFieldsView))
      
      List.empty[DataFrame]
    }
  }
}
