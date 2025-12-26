package com.experian.ind.prime.ingest.core.parser.commercial.service

import com.experian.ind.prime.ingest.core.Util.parser.commercial.DualLogger
import com.experian.ind.prime.ingest.core.parser.commercial.impl.FileParserImpl
import com.experian.ind.prime.ingest.core.parser.commercial.util.CommercialUtility
import com.experian.ind.prime.ingest.core.shared_models.PipelineContext
import org.apache.spark.sql.DataFrame

object FileParsingService {
  
  private lazy val logger = DualLogger(getClass)

  /**
   * Parse a file through header-trailer validation and record-level parsing
   * Orchestrates workflow: DF1 -> (validate rules) -> DF2 or DF3/DF4 (rejections)
   *
   * @param context  PipelineContext containing configuration, metadata, and Spark session
   * @param sourceDF Input DataFrame (DF1)
   * @return List[DataFrame] containing DF2 (parsed records) or DF3/DF4 (rejections)
   */
  def parsingFile(context: PipelineContext, sourceDF: DataFrame): List[DataFrame] = {
    // Extract all configuration upfront (before try block) to avoid mutable variable reassignment
    val fileLoadId = context.fileLoadId
    val fileFormat = context.fileFormatConfigModel.fileMetaData.format
    val inputFileName = context.metadata.fileName
    val inputFilePath = context.metadata.inputPath
    val outputPath = context.parserConfigModel.paths.outputPath

    try {
      logger.info(s"[SERVICE] Starting parsingFile: fileLoadId='$fileLoadId', format='$fileFormat', inputFileName='$inputFileName'")
      logger.debug(s"[SERVICE] Configuration: inputPath='$inputFilePath', outputPath='$outputPath'")
      
      // Validate paths before processing
      CommercialUtility.validateWithMessage(inputFilePath, inputFileName, outputPath) match {
        case Left(msg) =>
          logger.error(s"[SERVICE] Path validation failed for fileLoadId='$fileLoadId', format='$fileFormat': $msg")
          throw new IllegalArgumentException(msg)
        case Right(inputFile) =>
          logger.info(s"[SERVICE] Path validation successful. inputFile='${inputFile.getAbsolutePath}'")
      }

      val parser = new FileParserImpl(context)

      // Step 1: Evaluate file-level (header-trailer) rules
      logger.info(s"[SERVICE] Step 1/2: Evaluating file-level rules (header-trailer validation) for fileLoadId='$fileLoadId'")
      val ruleDfs = parser.parsingWithHeaderTrailer(sourceDF = sourceDF)

      if (ruleDfs.nonEmpty) {
        logger.warn(s"[SERVICE] File-level rules produced ${ruleDfs.size} rejection DataFrame(s) (DF3/DF4) for fileLoadId='$fileLoadId', format='$fileFormat'. Short-circuiting to rejection handling.")
        ruleDfs
      } else {
        // Step 2: Parse records (header-trailer rules passed)
        logger.info(s"[SERVICE] Step 2/2: File-level rules passed. Proceeding with record-level parsing for fileLoadId='$fileLoadId', format='$fileFormat'")
        val df2 = parser.parsingWithRecord(sourceDF = sourceDF)
        logger.info(s"[SERVICE] Record-level parsing completed successfully for fileLoadId='$fileLoadId'. Returning DF2 with ${df2.count()} rows")
        List(df2)
      }
    } catch {
      case ex: Exception =>
        logger.error(s"[SERVICE] Exception in parsingFile: fileLoadId='$fileLoadId', format='$fileFormat', inputFileName='$inputFileName', error='${ex.getMessage}'", ex)
        throw ex
    }
  }
}
