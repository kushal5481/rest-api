package com.experian.ind.prime.ingest.core.splitter.commercial.service

import com.experian.ind.prime.ingest.core.Util.parser.commercial.{DualLogger, Stopwatch}
import com.experian.ind.prime.ingest.core.shared_models.PipelineContext
import com.experian.ind.prime.ingest.core.splitter.commercial.impl.FileSplitterImpl
import com.experian.ind.prime.ingest.core.parser.commercial.util.CommercialConstants
import com.experian.ind.prime.ingest.core.splitter.commercial.util.{FixedLengthSplitter, PipeDelimitedSplitter}
import org.apache.spark.sql.DataFrame
import com.experian.ind.prime.ingest.core.parser.commercial.util.CommercialUtility
import java.io.File

/**
 * FileSplitterService - Handles file splitting into DATAFRAME-1
 * 
 * Workflow:
 * - [STEP 1] Validate input file and output paths
 * - [STEP 2] Route to format-specific splitter (pipe-delimited or fixed-length)
 * - [STEP 3] Split file into records
 * - [STEP 4] Optimize and create DATAFRAME-1
 */
object FileSplitterService {
  private lazy val logger = DualLogger(getClass)

  /**
   * Split input file into DATAFRAME-1 (raw records)
   * Supports pipe-delimited and fixed-length file formats
   *
   * @param context PipelineContext containing SparkSession, configurations, and file metadata
   * @return DataFrame DF1 containing split raw records
   */
  def splittingFile(context: PipelineContext): DataFrame = {
    val spark = context.spark
    val metadata = context.metadata
    val fileLoadId = context.fileLoadId
    val fileFormat = context.fileFormatConfigModel.fileMetaData.format.toUpperCase
    val parserConfigModel = context.parserConfigModel
    val fileFormatConfigModel = context.fileFormatConfigModel
    val inputFileName = metadata.fileName
    val inputFilePath = metadata.inputPath
    val outputPath = parserConfigModel.paths.outputPath

    val stopwatch = Stopwatch.createStarted()
    logger.info(s"[SPLITTER] Starting file splitting. fileLoadId=$fileLoadId, fileName=$inputFileName, format=$fileFormat")

    try {
      // ============== STEP 1: VALIDATE PATHS ==============
      logger.info(s"[STEP 1] Validating input file path and output path. fileLoadId=$fileLoadId")
      CommercialUtility.validateWithMessage(inputFilePath, inputFileName, outputPath) match {
        case Left(msg) =>
          logger.error(s"[STEP 1] Path validation failed: $msg")
          throw new IllegalArgumentException(msg)
        case Right(inputFile) =>
          logger.info(s"[STEP 1] Path validation successful. inputFile=${inputFile.getAbsolutePath}, outputPath=$outputPath")
      }

      // ============== STEP 2: SELECT SPLITTER ==============
      logger.info(s"[STEP 2] Selecting format-specific splitter. format=$fileFormat")
      val absoluteInputFilePath = new File(inputFilePath, inputFileName).getAbsolutePath
      
      val splittedRecords = try {
        fileFormat match {
          case x if x == CommercialConstants.FORMAT_FIXEDLENGTH.toUpperCase =>
            logger.info(s"[STEP 2] Using FixedLengthSplitter for format=$fileFormat")
            val splitter = new FixedLengthSplitter(fileFormatConfigModel)
            splitter.splitFile(absoluteInputFilePath)
          case x if x == CommercialConstants.FORMAT_PIPEDELIMITED.toUpperCase =>
            logger.info(s"[STEP 2] Using PipeDelimitedSplitter for format=$fileFormat")
            val splitter = new PipeDelimitedSplitter(fileFormatConfigModel)
            splitter.splitFile(absoluteInputFilePath)
          case other =>
            throw new IllegalArgumentException(s"Unsupported file format: $other")
        }
      } catch {
        case ex: Exception =>
          logger.error(s"[STEP 2] Exception in splitter selection for format=$fileFormat: ${ex.getMessage}", ex)
          throw ex
      }
      logger.info(s"[STEP 2] File splitting completed. recordCount=${splittedRecords.length}")

      // ============== STEP 3 & 4: OPTIMIZE AND CREATE DF1 ==============
      logger.info(s"[STEP 3] Creating FileSplitterImpl and optimizing records. fileLoadId=$fileLoadId")
      val splittingService = new FileSplitterImpl(context)
      
      val dataFrame_1 = try {
        splittingService.splittingWithOptimization(
          splittedRecords,
          outputPath,
          inputFileName,
          fileLoadId
        )
      } catch {
        case ex: Exception =>
          logger.error(s"[STEP 3] Exception in splittingWithOptimization for fileLoadId=$fileLoadId: ${ex.getMessage}", ex)
          throw ex
      }
      
      val df1RowCount = dataFrame_1.count().toInt
      logger.info(s"[STEP 3] DATAFRAME-1 created successfully. rowCount=$df1RowCount, outputPath=$outputPath")

      stopwatch.stop()
      logger.info(s"[SPLITTER] File splitting completed successfully. fileLoadId=$fileLoadId, format=$fileFormat, elapsedTime=${stopwatch.elapsed()}, df1RowCount=$df1RowCount")
      dataFrame_1
    } catch {
      case ex: Exception =>
        stopwatch.stop()
        logger.error(s"[SPLITTER] Exception in splittingFile. fileLoadId=$fileLoadId, fileName=$inputFileName, format=$fileFormat, elapsedTime=${stopwatch.elapsed()}: ${ex.getMessage}", ex)
        throw ex
    }
  }
}
