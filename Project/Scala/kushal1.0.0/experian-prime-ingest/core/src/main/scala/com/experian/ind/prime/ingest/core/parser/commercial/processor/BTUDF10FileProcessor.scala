package com.experian.ind.prime.ingest.core.parser.commercial.processor

import com.experian.ind.prime.ingest.core.Util.parser.commercial.{DataFrameUtils, DualLogger, Stopwatch}
import com.experian.ind.prime.ingest.core.parser.commercial.service.FileParsingService
import com.experian.ind.prime.ingest.core.shared_models.PipelineContext
import com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial._
import com.experian.ind.prime.ingest.core.splitter.commercial.service.FileSplitterService

import scala.util.Try

/**
 * BTUDF10 File Processor
 * Processes Commercial Bureau UCRF fixed-length/Pipe-Delimited files
 */
object BTUDF10FileProcessor {
  
  private lazy val logger = DualLogger(getClass)

  /**
   * Process a BTUDF10 file through splitting and parsing stages
   * 
   * Workflow:
   * - [STEP 1] Split input file into DATAFRAME-1 (raw records)
   * - [STEP 2] Parse DATAFRAME-1 into DATAFRAME-2 (valid records) + DATAFRAME-3/4 (rejections)
   * - [STEP 3] Preview results and unpersist DataFrames
   *
   * @param context PipelineContext containing configuration and file metadata
   * @return Try[ProcessingResult] with batch DataFrames and metadata
   */
  def processFile(context: PipelineContext): Try[ProcessingResult] = {

    val spark = context.spark
    val metadata = context.metadata
    val fileLoadId = context.fileLoadId
    val fileFormat = context.fileFormatConfigModel.fileMetaData.format
    val parserConfigModel = context.parserConfigModel
    val fileFormatConfigModel = context.fileFormatConfigModel
    val inputFileName = metadata.fileName
    val outputPath = parserConfigModel.paths.outputPath
    
    val splitterService = new com.experian.ind.prime.ingest.core.splitter.commercial.impl.FileSplitterImpl(context)
    val stopwatch = Stopwatch.createStarted()
    
    logger.info(s"[PROCESSOR] Starting BTUDF10 file processing. fileLoadId=$fileLoadId, fileName=$inputFileName, format=$fileFormat")

    Try {
      // ============== STEP 1: FILE SPLITTING ==============
      logger.info(s"[STEP 1] Splitting input file into DATAFRAME-1. fileLoadId=$fileLoadId, fileName=$inputFileName")
      val dataFrame_1 = FileSplitterService.splittingFile(context)
      val df1RowCount = dataFrame_1.count().toInt
      logger.info(s"[STEP 1] DATAFRAME-1 created successfully. rowCount=$df1RowCount")
      
      // ============== STEP 2: FILE PARSING ==============
      logger.info(s"[STEP 2] Parsing DATAFRAME-1 into DATAFRAME-2/3/4. fileLoadId=$fileLoadId, format=$fileFormat")
      val parsedDataFrames = FileParsingService.parsingFile(context, dataFrame_1)
      val batchDataFrames = dataFrame_1 :: parsedDataFrames
      logger.info(s"[STEP 2] Parsing completed successfully. producedDataFrames=${batchDataFrames.size} (DF1 + parsed results)")
      
      // Log detailed counts for parsed results
      if (parsedDataFrames.nonEmpty) {
        parsedDataFrames.zipWithIndex.foreach { case (df, idx) =>
          val dfName = if (idx == 0) "DF2 (parsed)" else if (idx == 1) "DF3 (rejections)" else s"DF${idx + 2}"
          val rowCount = df.count().toInt
          logger.info(s"[STEP 2] $dfName rowCount=$rowCount")
        }
      }
      
      // ============== STEP 3: PREVIEW & CLEANUP ==============
      logger.info(s"[STEP 3] Previewing and unpersisting DataFrames. fileLoadId=$fileLoadId")
      DataFrameUtils.previewDataFrames(batchDataFrames, limit = 20, recordLength = 50)
      logger.info(s"[STEP 3] DataFrame preview completed")
      
      // Create result before unpersisting
      val result = ProcessingResult(
        recordCount = df1RowCount,
        outputPath = outputPath,
        success = true,
        batchDataFrames = batchDataFrames
      )
      
      // Unpersist all DataFrames to free memory
      logger.info(s"[STEP 3] Unpersisting ${batchDataFrames.size} DataFrames to free memory. fileLoadId=$fileLoadId")
      batchDataFrames.foreach { df =>
        splitterService.unpersistDataFrame(df)
      }
      logger.info(s"[STEP 3] All ${batchDataFrames.size} DataFrames unpersisted successfully")
      
      stopwatch.stop()
      logger.info(s"[PROCESSOR] BTUDF10 file processing completed successfully. fileLoadId=$fileLoadId, elapsedTime=${stopwatch.elapsed()}, totalDataFrames=${batchDataFrames.size}, df1RowCount=$df1RowCount")
      result
    } recover {
      case ex: Exception =>
        stopwatch.stop()
        logger.error(s"[PROCESSOR] Exception in file processing. fileLoadId=$fileLoadId, fileName=$inputFileName, format=$fileFormat, elapsedTime=${stopwatch.elapsed()}: ${ex.getMessage}", ex)
        throw ex
    }
  }
}
