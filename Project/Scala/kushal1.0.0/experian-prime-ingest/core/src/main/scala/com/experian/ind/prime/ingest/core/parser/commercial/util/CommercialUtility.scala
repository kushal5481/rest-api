package com.experian.ind.prime.ingest.core.parser.commercial.util

import com.experian.ind.prime.ingest.core.shared_models.PipelineContext
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import com.experian.ind.prime.ingest.core.Util.parser.commercial.{DualLogger, Stopwatch}
import com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial.RuleDefinition

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.io.File

object CommercialUtility {
    private lazy val logger = DualLogger(getClass)
  
  /**
    * Returns the current datetime formatted using the provided pattern.
    * Example pattern: "dd-MM-yyyy HH:mm:ss"
    */
  def nowFormatted(pattern: String, zoneId: ZoneId = ZoneId.systemDefault()): String = {
    val formatter = DateTimeFormatter.ofPattern(pattern)
    LocalDateTime.now(zoneId).format(formatter)
  }

  /**
    * Formats the given LocalDateTime using the provided pattern.
    */
  def format(dateTime: LocalDateTime, pattern: String): String = {
    val formatter = DateTimeFormatter.ofPattern(pattern)
    dateTime.format(formatter)
  }

  /**
   * Validates input file path, input file name, existence of the input file, and output path.
   * Returns the resolved `File` pointing to the input file when all checks pass.
   * Throws IllegalArgumentException with descriptive messages on failures.
   */
  def validateInputAndOutput(inputFilePath: String, inputFileName: String, outputPath: String): File = {
    if (!new File(inputFilePath).exists()) {
      throw new IllegalArgumentException(s"Input file path not found: $inputFilePath")
    }

    if (inputFileName == null || inputFileName.trim.isEmpty) {
      throw new IllegalArgumentException(s"Input file name is null or empty")
    }

    val inputFile = new File(inputFilePath, inputFileName)
    if (!inputFile.exists()) {
      throw new IllegalArgumentException(s"Input file not found: ${inputFile.getAbsolutePath}")
    }
    if (!inputFile.isFile) {
      throw new IllegalArgumentException(s"Input path is not a file: ${inputFile.getAbsolutePath}")
    }
    if (!inputFile.canRead) {
      throw new IllegalArgumentException(s"Input file is not readable (access denied): ${inputFile.getAbsolutePath}")
    }

    if (!new File(outputPath).exists()) {
      throw new IllegalArgumentException(s"Output path not found: $outputPath")
    }

    inputFile
  }

  /**
   * Non-throwing variant. Returns Right(inputFile) when valid, otherwise Left(errorMessage).
   */
  def validateWithMessage(inputFilePath: String, inputFileName: String, outputPath: String): Either[String, File] = {
    if (!new File(inputFilePath).exists()) {
      return Left(s"Input file path not found: $inputFilePath")
    }
    if (inputFileName == null || inputFileName.trim.isEmpty) {
      return Left("Input file name is null or empty")
    }
    val inputFile = new File(inputFilePath, inputFileName)
    if (!inputFile.exists()) {
      return Left(s"Input file not found: ${inputFile.getAbsolutePath}")
    }
    if (!inputFile.isFile) {
      return Left(s"Input path is not a file: ${inputFile.getAbsolutePath}")
    }
    if (!inputFile.canRead) {
      return Left(s"Input file is not readable (access denied): ${inputFile.getAbsolutePath}")
    }
    if (!new File(outputPath).exists()) {
      return Left(s"Output path not found: $outputPath")
    }
    Right(inputFile)
  }

  /**
   * Boolean convenience. Returns true when valid; false otherwise.
   */
  def isValid(inputFilePath: String, inputFileName: String, outputPath: String): Boolean = {
    validateWithMessage(inputFilePath, inputFileName, outputPath).isRight
  }

  /**
    * Resolves the Segment_TypeofLine string from schema JSON based on a logical kind
    * (e.g., CommercialConstants.HEADER/FOOTER/RECORD). Falls back to the provided kind
    * if schema-driven resolution fails.
    */
  def fetchSegmentTypeofLine(context: PipelineContext, kind: String): String = {
    try {
      val segments = context.fileFormatConfigModel.Segments

      def byTag(tag: String): Option[String] =
        Option(tag)
          .flatMap(t => segments.find(s => Option(s.Segment_Tag).exists(_.equalsIgnoreCase(t))))
          .flatMap(s => Option(s.Segment_TypeofLine))

      if (kind.equalsIgnoreCase(CommercialConstants.HEADER)) {
        val startTag = context.fileFormatConfigModel.fileStructure.startSegment
        byTag(startTag).getOrElse(CommercialConstants.HEADER)
      } else if (kind.equalsIgnoreCase(CommercialConstants.FOOTER)) {
        val endTag = context.fileFormatConfigModel.fileStructure.endSegment
        byTag(endTag).getOrElse(CommercialConstants.FOOTER)
      } else if (kind.equalsIgnoreCase(CommercialConstants.RECORD)) {
        val recordTag = context.fileFormatConfigModel.recordStructure.recordStartSegment
        byTag(recordTag).getOrElse(CommercialConstants.RECORD)
      } else {
        // Unknown kind; return as-is to avoid breaking callers
        kind
      }
    } catch {
      case _: Throwable => kind
    }
  }

  /**
    * Fetches a set of segment properties from the loaded schema by segment tag.
    * Callers provide the `segmentTag` (e.g., "HD", "BS") and a list of property names
    * (keys) to retrieve from that segment model (e.g., "Segment_IsMandatory", "Segment_Maximum_Length").
    * Returns a Map of key -> value (as String) where missing keys map to null.
    */
  def fetchSegmentProperties(context: PipelineContext, segmentTag: String, keys: Seq[String]): Map[String, String] = {
    try {
      val maybeSeg = Option(context.fileFormatConfigModel.Segments)
        .flatMap(_.find(s => Option(s.Segment_Tag).exists(_.equalsIgnoreCase(segmentTag))))

      maybeSeg.map { seg =>
        keys.map { key =>
          val fieldOpt = seg.getClass.getDeclaredFields.find(_.getName.equalsIgnoreCase(key))
          val valueStr = fieldOpt.map { f =>
            f.setAccessible(true)
            Option(f.get(seg)).map(_.toString).orNull
          }.orNull
          key -> valueStr
        }.toMap
      }.getOrElse(Map.empty[String, String])
    } catch {
      case _: Throwable => Map.empty[String, String]
    }
  }

  /**
    * Creates a one-row Spark SQL temp view containing selected segment properties
    * from the schema for the given `segmentTag`.
    *
    * - `keys` are the schema field names to pull from the segment (e.g., "Segment_IsMandatory").
    * - `aliases` are the desired column names in the temp view (e.g., "segment_is_mandatory").
    *   If `aliases` length doesn't match `keys`, the original `keys` are used as column names.
    * - `viewName` is the name of the temp view to create.
    *
    * Values are inserted as strings; callers can CAST in SQL as needed.
    */
  def createSegmentConstraintsView(
      context: PipelineContext,
      segmentTag: String,
      keys: Seq[String],
      aliases: Seq[String],
      viewName: String
  ): Unit = {
    val props = fetchSegmentProperties(context, segmentTag, keys)
    val cols = if (aliases != null && aliases.size == keys.size) aliases else keys
    val values: Seq[Any] = keys.map(k => props.getOrElse(k, null))

    // Build a one-row DataFrame using Row + StructType to support dynamic column count
    val row = Row.fromSeq(values)
    val schema = StructType(cols.map(c => StructField(c, StringType, nullable = true)))
    val df = context.spark.createDataFrame(context.spark.sparkContext.parallelize(Seq(row)), schema)
    df.createOrReplaceTempView(viewName)
  }

  /**
    * Creates a temp view with per-field schema for a segment, including fixed-length offsets.
    * View columns: field_index, field_tag, field_name, field_is_mandatory, field_max_length,
    * field_occurrence, field_expected_values (comma-separated), start_pos, end_pos.
    */
  def createSegmentFieldsView(
      context: PipelineContext,
      segmentTag: String,
      viewName: String
  ): Unit = {
    try {
      val maybeSeg = Option(context.fileFormatConfigModel.Segments)
        .flatMap(_.find(s => Option(s.Segment_Tag).exists(_.equalsIgnoreCase(segmentTag))))

      val fields = maybeSeg.flatMap(seg => Option(seg.Fields)).getOrElse(Seq.empty)

      // Compute fixed-length start/end positions cumulatively (1-based positions for SUBSTRING)
      // Skip initial 2-character segment code (e.g., "HD", "BS") in RawRecord
      var cursor = 3
      val rows: Seq[Row] = fields.map { f =>
        val idx = Option(f.Field_Index).getOrElse(0)
        val tag = Option(f.Field_Tag).orNull
        val name = Option(f.Field_Name).orNull
        val isMand = Option(f.Field_IsMandatory).map(_.toString).orNull
        val maxLen = Option(f.Field_Maximum_Length).map(_.toString).orNull
        val occ = Option(f.Field_Occurrence).orNull
        val expectedVals = Option(f.Field_Expected_Values).map(_.mkString(",")).orNull
        val start = cursor
        val lenInt = Option(f.Field_Maximum_Length).getOrElse(0)
        val end = start + math.max(lenInt, 0) - 1
        cursor = end + 1
        Row(idx.toString, tag, name, isMand, maxLen, occ, expectedVals, start.toString, end.toString)
      }

      val schema = StructType(Seq(
        StructField("field_index", StringType, true),
        StructField("field_tag", StringType, true),
        StructField("field_name", StringType, true),
        StructField("field_is_mandatory", StringType, true),
        StructField("field_max_length", StringType, true),
        StructField("field_occurrence", StringType, true),
        StructField("field_expected_values", StringType, true),
        StructField("start_pos", StringType, true),
        StructField("end_pos", StringType, true)
      ))

      val df = context.spark.createDataFrame(context.spark.sparkContext.parallelize(rows), schema)
      df.createOrReplaceTempView(viewName)
    } catch {
      case ex: Throwable =>
        logger.error(s"Failed to create fields view for segment $segmentTag: ${ex.getMessage}", ex)
        throw ex
    }
  }

  /**
    * Creates a temp view for pipe-delimited layouts where fields are tokenized by the delimiter from JSON config.
    * View columns: field_index, field_tag, field_name, field_is_mandatory, field_max_length,
    * field_occurrence, field_expected_values, index_in_pipe.
    * For pipe-delimited layouts, the `Fields` schema indexes map directly to the split token positions,
    * so `index_in_pipe = Field_Index`.
    * @param delimiter The delimiter character(s) read from JSON configuration
    */
  def createSegmentPipeFieldsView(
      context: PipelineContext,
      segmentTag: String,
      viewName: String,
      delimiter: String
  ): Unit = {
    try {
      val maybeSeg = Option(context.fileFormatConfigModel.Segments)
        .flatMap(_.find(s => Option(s.Segment_Tag).exists(_.equalsIgnoreCase(segmentTag))))

      val fields = maybeSeg.flatMap(seg => Option(seg.Fields)).getOrElse(Seq.empty)

      val segmentOffset = 0 // pipe-delimited: Field_Index maps directly to split token position
      val rows: Seq[Row] = fields.map { f =>
        val idx = Option(f.Field_Index).getOrElse(0)
        val tag = Option(f.Field_Tag).orNull
        val name = Option(f.Field_Name).orNull
        val isMand = Option(f.Field_IsMandatory).map(_.toString).orNull
        val maxLen = Option(f.Field_Maximum_Length).map(_.toString).orNull
        val occ = Option(f.Field_Occurrence).orNull
        val expectedVals = Option(f.Field_Expected_Values).map(_.mkString(",")).orNull
        val indexInPipe = idx + segmentOffset
        Row(
          idx.toString,
          tag,
          name,
          isMand,
          maxLen,
          occ,
          expectedVals,
          indexInPipe.toString
        )
      }

      val schema = StructType(Seq(
        StructField("field_index", StringType, true),
        StructField("field_tag", StringType, true),
        StructField("field_name", StringType, true),
        StructField("field_is_mandatory", StringType, true),
        StructField("field_max_length", StringType, true),
        StructField("field_occurrence", StringType, true),
        StructField("field_expected_values", StringType, true),
        StructField("index_in_pipe", StringType, true)
      ))

      val df = context.spark.createDataFrame(context.spark.sparkContext.parallelize(rows), schema)
      df.createOrReplaceTempView(viewName)
      
      // Also expose delimiter as a single-row view for SQL rules to reference
      // Provide both regex-escaped and literal forms for split() and replace() use cases
      val delimiterLiteral = delimiter
      // Use Pattern.quote to safely escape any delimiter for regex usage (e.g., '|')
      val delimiterRegex = java.util.regex.Pattern.quote(delimiter)
      val delimiterViewName = s"${viewName}_delimiter"
      val delimiterRow = Row(delimiterRegex, delimiterLiteral)
      val delimiterSchema = StructType(Seq(
        StructField("delimiter_regex", StringType, true),
        StructField("delimiter_literal", StringType, true)
      ))
      val delimiterDF =  context.spark.createDataFrame(context.spark.sparkContext.parallelize(Seq(delimiterRow)), delimiterSchema)
      delimiterDF.createOrReplaceTempView(delimiterViewName)
    } catch {
      case ex: Throwable =>
        logger.error(s"Failed to create pipe fields view for segment $segmentTag: ${ex.getMessage}", ex)
        throw ex
    }
  }

  /**
   * Writes a DataFrame as a single CSV file into `outputDirPath` named `outputFileName.csv`.
   * Uses a temp folder and renames the Spark part file. Creates `outputDirPath` if needed.
   */
  def writeDataFrameToCSV(df: DataFrame, fileLoadId: Long, outputDirPath: String, outputFileName: String): Unit = {
    val stopwatch = Stopwatch.createStarted()
    val fileLoadSubDir = new java.io.File(outputDirPath, fileLoadId.toString)
    val fileLoadSubDirPath = fileLoadSubDir.getAbsolutePath
    logger.info(s"Writing DataFrame to CSV at: $fileLoadSubDirPath as $outputFileName.csv")
    val tempPath = new java.io.File(fileLoadSubDir, s"${outputFileName}_temp").getAbsolutePath
    var tempDir: java.io.File = null
    try {
      // Replace all empty strings with nulls for string columns
      val stringCols = df.schema.fields.collect { case f if f.dataType == org.apache.spark.sql.types.StringType => f.name }
      val dfForCsv = if (stringCols.nonEmpty) {
        df.na.replace(stringCols, Map("" -> null))
      } else df

      dfForCsv.coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .option("header", "true")
        .option("quote", "\"")
        .option("escape", "\"")
        .option("ignoreTrailingWhiteSpace", "false")
        .option("nullValue", "")
        .csv(tempPath)

      tempDir = new java.io.File(tempPath)
      val partFile = tempDir.listFiles().find(_.getName.startsWith("part-"))
      if (partFile.isDefined) {
        if (!fileLoadSubDir.exists()) fileLoadSubDir.mkdirs()
        val finalFile = new java.io.File(fileLoadSubDir, s"${outputFileName}.csv")
        partFile.get.renameTo(finalFile)
        logger.info(s"Output file created: ${finalFile.getAbsolutePath}")
      } else {
        logger.warn(s"No part-* file found under temp path: $tempPath")
      }
      logger.info(s"DataFrame write completed in ${stopwatch.elapsed()}")
    } catch {
      case ex: Throwable =>
        logger.error(s"Failed to write DataFrame to CSV at $fileLoadSubDirPath: ${ex.getMessage}", ex)
        throw ex
    } finally {
      // Always attempt to clean up temp directory
      if (tempDir == null) tempDir = new java.io.File(tempPath)
      if (tempDir.exists() && tempDir.isDirectory) {
        tempDir.listFiles().foreach(f => try f.delete() catch { case _: Throwable => () })
        try tempDir.delete() catch { case _: Throwable => () }
      }
      stopwatch.stop()
    }
  }

  // Normalize optional array of strings (from JSON arrays) to a CSV string or null if empty/blank.
    def normalizeEmptyArrayString(opt: Option[Seq[String]]): String = {
      opt match {
        case None => null
        case Some(values) =>
          val cleaned = values.collect { case s if s != null => s.trim }.filter(_.nonEmpty)
          if (cleaned.isEmpty) null else cleaned.mkString(",")
      }
    }

    def isFatalFileRule(r: RuleDefinition, context: PipelineContext): Boolean = {
      val isFatal = r.error_type.exists(_.equalsIgnoreCase(CommercialConstants.ERRORTYPE_FATAL))
      val levelAllowed = r.rule_level.exists(_.equalsIgnoreCase(CommercialConstants.RULELEVEL_FILE))
      val ruleAction = r.rule_action.exists(_.equalsIgnoreCase(CommercialConstants.RULEACTION_REJECT))

      // Prefer schema-driven start/end segment tags; fallback to common defaults (HD/TS)
      val schemaStartTag = try {
        Option(context).flatMap(c => Option(c.fileFormatConfigModel))
          .flatMap(m => Option(m.fileStructure)).flatMap(fs => Option(fs.startSegment))
      } catch { case _: Throwable => None }
      val schemaEndTag = try {
        Option(context).flatMap(c => Option(c.fileFormatConfigModel))
          .flatMap(m => Option(m.fileStructure)).flatMap(fs => Option(fs.endSegment))
      } catch { case _: Throwable => None }

      val allowedTags: Set[String] = (schemaStartTag.toSeq ++ schemaEndTag.toSeq) match {
        case Nil => Set("HD", "TS")
        case seq => seq.toSet
      }

      // `segment` may be an array; allow match if any provided segment matches allowed tags
      val segmentAllowed = r.segment.exists(arr => arr.exists(seg => allowedTags.exists(_.equalsIgnoreCase(seg))))
      isFatal && levelAllowed && ruleAction && segmentAllowed
    }

    /**
      * Generic utility to cleanup temporary Spark SQL views and their cached data
      * Handles both cached and non-cached views gracefully without throwing errors
      * Uses SQL DROP VIEW IF EXISTS to safely handle non-existent views
      */
    def cleanupTemporaryViews(spark: org.apache.spark.sql.SparkSession, viewNames: List[String]): Unit = {
      try {
        for (viewName <- viewNames) {
          try {
            // Uncache if cached
            if (spark.catalog.isCached(viewName)) {
              spark.catalog.uncacheTable(viewName)
              logger.debug(s"Uncached view: $viewName")
            }
            // Drop the temporary view using SQL to handle non-existent views gracefully
            spark.sql(s"DROP VIEW IF EXISTS $viewName")
            logger.debug(s"Dropped temporary view: $viewName")
          } catch {
            case ex: Throwable =>
              // Log warning but continue cleanup for other views
              logger.warn(s"Could not cleanup view '$viewName': ${ex.getMessage}")
          }
        }
        if (viewNames.nonEmpty) {
          logger.info(s"Cleaned up ${viewNames.size} temporary views and cached data")
        }
      } catch {
        case ex: Throwable =>
          logger.error(s"Error during cleanup of temporary views: ${ex.getMessage}", ex)
      }
    }

    def writeDataFrame(context: PipelineContext, dataframeName: String, dataFrame: DataFrame): Unit = {
    logger.info("Starting writeDataFrame")
    
    val inputFileName = context.metadata.fileName
    val outputPath = context.parserConfigModel.paths.outputPath
    val fileLoadId = context.fileLoadId
    
    try {
      val fileNameWithoutExt = Option(inputFileName)
        .filter(_.trim.nonEmpty)
        .map(_.replaceAll("\\.[^.]*$", ""))
        .getOrElse("output")
      val outputFileName = s"${fileNameWithoutExt}${CommercialConstants.UNDERSCORESIGN}${fileLoadId}${CommercialConstants.UNDERSCORESIGN}${dataframeName}"
      
      logger.info(s"[writeDataFrame] Writing DF to CSV: outputPath='$outputPath', outputFileName='$outputFileName'")
      writeDataFrameToCSV(dataFrame, fileLoadId, outputPath, outputFileName)
      logger.info(s"[writeDataFrame] DF written successfully to output")
    } catch {
      case ex: Exception =>
        logger.error(s"[writeDataFrame] Error writing DF to CSV: ${ex.getMessage}", ex)
        throw ex
    }
  }
}