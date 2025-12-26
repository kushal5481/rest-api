package com.experian.ind.prime.ingest.core.splitter.commercial.impl

import com.experian.ind.prime.ingest.core.Util.parser.commercial.{DualLogger, IDGenerator, Stopwatch}
import com.experian.ind.prime.ingest.core.shared_models.PipelineContext
import com.experian.ind.prime.ingest.core.parser.commercial.util.{CommercialConstants, CommercialUtility}
import com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import scala.collection.mutable.ListBuffer

/**
 * Optimized File Processing Service with multi-threading support
 * Generates DataFrame-1 in the specified format with FileLoadId, BatchID, and RecordID
 */
class FileSplitterImpl(context: PipelineContext) {
  private lazy val logger = DualLogger(getClass)

  private val spark = context.spark
  private val metadata = context.metadata
  private val fileLoadId = context.fileLoadId
  private val fileFormat = context.fileFormatConfigModel.fileMetaData.format
  private val parserConfigModel = context.parserConfigModel
  private val fileFormatConfigModel = context.fileFormatConfigModel
  private val formatConfig: FileFormatConfig = fileFormatConfigModel
  private val parallelism = parserConfigModel.processing.flatMap(_.parallelism).getOrElse(4)
  private val partitions = parserConfigModel.processing.flatMap(_.partitions).getOrElse(4)
  private val batchSize = parserConfigModel.processing.flatMap(_.batchSize).getOrElse(1000)
  
  private val SEPARATOR = "=" * 80

  private def processRecordsToDF1(records: List[ParsedRecord], inputFileName: String, fileLoadId: Long): DataFrame = {
    val stepStopwatch = Stopwatch.createStarted()
    logger.info(s"[STEP 1] Processing ${records.size} records to DataFrame with optimization")
    
    val batchId = IDGenerator.generateBatchId()
    logger.info(s"[STEP 1] Generated IDs - FileLoadId=$fileLoadId, BatchID=$batchId")
    
    val context = ProcessingContext(fileLoadId, batchId, 0)
    val startSegment = formatConfig.fileStructure.startSegment
    val endSegment = formatConfig.fileStructure.endSegment
    val recordStartSegment = formatConfig.recordStructure.recordStartSegment
    logger.info(s"[STEP 1] File structure - Start=$startSegment, End=$endSegment, RecordStart=$recordStartSegment")
    
    val recordGroups = groupRecordsByFileStructure(records, startSegment, endSegment, recordStartSegment)
    logger.info(s"[STEP 1] Grouped into ${recordGroups.size} record groups. Distribution: ${recordGroups.map(_.records.size).groupBy(identity).mapValues(_.size)}")
    
    val dataFrameRecords = recordGroups.map { group =>
      createDataFrameRecordForGroup(group, context, fileLoadId, batchId)
    }
    logger.info(s"[STEP 1] Created ${dataFrameRecords.size} DataFrame records with sequential counters")
    
    // Ensure deterministic ordering by numeric prefix of RecordID (e.g., "1-...", "2-...")
    // Single optimized transformation chain with caching at the end
    val df = spark.createDataFrame(dataFrameRecords)
      .repartition(partitions)
      .withColumn("RecordIdNum", regexp_extract(col("RecordID"), "^(\\d+)", 1).cast("int"))
      .orderBy(col("RecordIdNum"))
      .drop("RecordIdNum")
      .cache()  // Single cache point instead of double caching
    
    val df1RowCount = df.count()
    logger.info(s"[STEP 1] DataFrame-1 finalized: rowCount=$df1RowCount, partitions=$partitions, cached=true, elapsedTime=${stepStopwatch.elapsed()}")
    stepStopwatch.stop()
    df
  }  

  private def groupRecordsByFileStructure(
    records: List[ParsedRecord],
    startSegment: String,
    endSegment: String,
    recordStartSegment: String
  ): List[RecordGroup] = {
    val groups = ListBuffer[RecordGroup]()
    var currentGroup = ListBuffer[ParsedRecord]()
    var currentType = "UNKNOWN"
    records.foreach { record =>
      record.segmentTag match {
        case tag if tag == startSegment =>
          if (currentGroup.nonEmpty) {
            groups += RecordGroup(currentType, currentGroup.toList)
            currentGroup.clear()
          }
          currentGroup += record
          currentType = (CommercialUtility.fetchSegmentTypeofLine(context, CommercialConstants.HEADER)).toUpperCase
        case tag if tag == recordStartSegment =>
          if (currentGroup.nonEmpty) {
            groups += RecordGroup(currentType, currentGroup.toList)
            currentGroup.clear()
          }
          currentGroup += record
          currentType = (CommercialUtility.fetchSegmentTypeofLine(context, CommercialConstants.RECORD)).toUpperCase
        case tag if tag == endSegment =>
          if (currentGroup.nonEmpty) {
            groups += RecordGroup(currentType, currentGroup.toList)
            currentGroup.clear()
          }
          currentGroup += record
          currentType = (CommercialUtility.fetchSegmentTypeofLine(context, CommercialConstants.FOOTER)).toUpperCase
        case _ =>
          currentGroup += record
      }
    }
    if (currentGroup.nonEmpty) {
      groups += RecordGroup(currentType, currentGroup.toList)
    }
    groups.toList
  }

  private def getSegmentName(segmentTag: String): String = {
    formatConfig.Segments.find(_.Segment_Tag == segmentTag)
      .map(_.Segment_Name)
      .getOrElse("Unknown")
  }

  private def createDataFrameRecordForGroup(
    group: RecordGroup,
    context: ProcessingContext,
    fileLoadId: Long,
    batchId: Int
  ): DF1RawRecords = {
    synchronized {
      try {
        val recordId = context.nextRecordId()
        val firstSegmentTag = group.records.head.segmentTag
        val typeOfLine = group.records.head.Fields.getOrElse("Segment_TypeofLine", {
          val fallback = getSegmentName(firstSegmentTag)
          logger.warn(s"[GROUP] TypeofLine not found for segment=$firstSegmentTag, using fallback=$fallback")
          fallback
        })
        val rawRecord = group.records.map(_.rawLine).mkString("\n")
        val recordInsertionTime = CommercialUtility.nowFormatted(CommercialConstants.DATETIMEFORMAT)
        val splitterStatus: String = null
        DF1RawRecords(
          FileLoadID = fileLoadId,
          BatchID = batchId,
          RecordID = recordId,
          Type_of_Line = typeOfLine,
          RawRecord = rawRecord,
          RecordInsertionTime = recordInsertionTime,
          Splitter_Status = splitterStatus
        )
      } catch {
        case ex: Exception =>
          logger.error(s"[GROUP] Exception creating DataFrame record for segment=${group.records.head.segmentTag}: ${ex.getMessage}", ex)
          throw ex
      }
    }
  }

  def splittingWithOptimization(records: List[ParsedRecord], outputPath: String, inputFileName: String, fileLoadId: Long): DataFrame = {
    val overallStopwatch = Stopwatch.createStarted()
    logger.info(SEPARATOR)
    logger.info(s"[SPLITTER] Starting optimized file splitting. fileLoadId=$fileLoadId, fileName=$inputFileName, format=$fileFormat")
    logger.info(s"[SPLITTER] Configuration - parallelism=$parallelism, partitions=$partitions, batchSize=$batchSize")
    logger.info(s"[SPLITTER] Input records=${records.size}")
    logger.info(SEPARATOR)
    
    val df1 = processRecordsToDF1(records, inputFileName, fileLoadId)
    logger.info(s"[SPLITTER] Writing DataFrame-1 to persistent storage. fileLoadId=$fileLoadId")
    CommercialUtility.writeDataFrame(context, context.parserConfigModel.dataFrames.dataframe1, df1)
    logger.info(s"[SPLITTER] DataFrame-1 persisted successfully")

    val summary = generateSummary(df1)
    logger.info(s"[SPLITTER] Processing Summary:")
    summary.show(false)
    
    overallStopwatch.stop()
    logger.info(s"[SPLITTER] File splitting completed successfully. fileLoadId=$fileLoadId, totalTime=${overallStopwatch.elapsed()}")
    logger.info(s"[SPLITTER] REMINDER: Call unpersistDataFrame(df1) after all downstream operations complete to free cached memory")
    logger.info(SEPARATOR)
    
    // Note: Caller is responsible for unpersisting df1 after all downstream operations complete
    df1
  }
  
  /**
   * Unpersist the cached DataFrame to free memory
   * Should be called after all operations on the DataFrame are complete
   * 
   * @param df DataFrame to unpersist (typically DataFrame-1)
   */
  def unpersistDataFrame(df: DataFrame): Unit = {
    try {
      df.unpersist(blocking = false)
      logger.info(s"[CLEANUP] DataFrame unpersisted successfully. fileLoadId=$fileLoadId")
    } catch {
      case ex: Exception =>
        logger.warn(s"[CLEANUP] Failed to unpersist DataFrame: ${ex.getMessage}")
    }
  }

  private def generateSummary(df: DataFrame): DataFrame = {
    df.agg(
      count("*").as("TotalRecords"),
      countDistinct("FileLoadId").as("UniqueFileLoadIDs"),
      countDistinct("BatchID").as("UniqueBatchIDs"),
      countDistinct("RecordID").as("UniqueRecordIDs")
    )
  }
}
