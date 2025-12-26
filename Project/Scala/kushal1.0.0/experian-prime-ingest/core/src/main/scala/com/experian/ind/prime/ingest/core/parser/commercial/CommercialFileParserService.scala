package com.experian.ind.prime.ingest.core.parser.commercial

import com.experian.ind.prime.ingest.core.Util.parser.commercial.{DualLogger, FileLogger, Stopwatch}
import com.experian.ind.prime.ingest.core.parser.commercial.processor.{BTUDF10FileProcessor, BTUDF30FileProcessor}
import com.experian.ind.prime.ingest.core.shared_models.PipelineContext
import com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial._
import com.experian.ind.prime.ingest.core.parser.commercial.util.CommercialConstants

import scala.util.{Failure, Success, Try}

/**
 * CommercialFileParserService - Main Entry Point for File Processing
 * This is the primary entry point for processing UCRF files based on metadata configuration.
 * It reads metadata properties and routes to appropriate processor based on industry type.
 */
object CommercialFileParserService {
  
  private val LOG_SEPARATOR = "=" * 80
  
  // Lazy logger initialization - will be created AFTER setupLogFilePath() sets the system property
  private lazy val logger = DualLogger(getClass)  

  /**
   * Start file parsing workflow
   * 
   * Workflow:
   * - [INIT] Log execution context
   * - [ROUTE] Route to appropriate processor based on industry/version
   * - [RESULT] Handle processing result
   * - [CLEANUP] Cleanup resources
   *
   * @param context PipelineContext containing all configuration and metadata
   */
  def startFileParsing(context: PipelineContext): Unit = {
    logger.info(LOG_SEPARATOR)
    logger.info("Starting PreDP Commercial File Parser Service...")
    logger.info(LOG_SEPARATOR)
    
    val spark = context.spark
    val metadata = context.metadata
    val fileLoadId = context.fileLoadId
    val fileName = metadata.fileName
    val industry = metadata.industry
    val version = metadata.fileVersion
    
    val stopwatch = Stopwatch.createStarted()

    logger.info(s"[INIT] Execution context: fileLoadId=$fileLoadId, fileName=$fileName, industry=$industry, version=$version")
    
    try {
      // Route to appropriate processor
      logger.info(s"[ROUTE] Routing to processor for industry='$industry', version='$version'")
      val result = routeToProcessor(context)
      
      // Handle processing result
      result match {
        case Success(processingResult) =>
          logger.info(LOG_SEPARATOR)
          logger.info("[RESULT] File Processing Completed Successfully!")
          logger.info(s"[RESULT] Industry: $industry, Version: $version")
          logger.info(s"[RESULT] Input File: $fileName")
          logger.info(s"[RESULT] Total Records Processed: ${processingResult.recordCount}")
          logger.info(s"[RESULT] Output Path: ${processingResult.outputPath}")
          logger.info(s"[RESULT] Output DataFrames: ${processingResult.batchDataFrames.size}")
          logger.info(s"[RESULT] Elapsed Time: ${stopwatch.elapsed()}")
          logger.info(LOG_SEPARATOR)
          
        case Failure(exception) =>
          logger.error(LOG_SEPARATOR)
          logger.error("[RESULT] File Processing Failed!")
          logger.error(s"[RESULT] Industry: $industry, Version: $version, File: $fileName")
          logger.error(s"[RESULT] Error: ${exception.getMessage}", exception)
          logger.error(s"[RESULT] Elapsed Time: ${stopwatch.elapsed()}")
          logger.error(LOG_SEPARATOR)
      }
      
    } catch {
      case e: Exception =>
        logger.error(LOG_SEPARATOR)
        logger.error("[ERROR] Fatal Error in PreDP File Parser!")
        logger.error(s"[ERROR] Context: fileLoadId=$fileLoadId, fileName=$fileName, industry=$industry, version=$version")
        logger.error(s"[ERROR] Exception: ${e.getMessage}", e)
        logger.error(s"[ERROR] Elapsed Time: ${stopwatch.elapsed()}")
        logger.error(LOG_SEPARATOR)
        
    } finally {
      // Cleanup resources
      logger.info("[CLEANUP] Starting resource cleanup...")
      if (spark != null) {
        logger.info("[CLEANUP] Clearing Spark cache...")
        try {
          spark.catalog.clearCache()
          logger.info("[CLEANUP] Spark cache cleared")
          
          logger.info("[CLEANUP] Stopping Spark Session...")
          spark.stop()
          logger.info("[CLEANUP] Spark Session stopped successfully")
          
          // Give Windows time to release file handles
          Thread.sleep(1000)
        } catch {
          case e: Exception =>
            logger.warn(s"[CLEANUP] Warning during Spark shutdown: ${e.getMessage}")
        }
      }
      
      stopwatch.stop()
      logger.info(s"[CLEANUP] Total Execution Time: ${stopwatch.elapsed()}")
      logger.info("[CLEANUP] Closing file logger...")
      FileLogger.close()
      logger.info("[CLEANUP] File logger closed")
      
      logger.info("PreDP Commercial File Parser Service Ended")
      logger.info(LOG_SEPARATOR)
    }
  }
  
  /**
   * Route to appropriate processor based on industry type and version
   * 
   * Supported configurations:
   * - COMMERCIAL + 1.0 → BTUDF10FileProcessor
   * - COMMERCIAL + 3.0 → BTUDF30FileProcessor
   * - CONSUMER (any version) → Not yet implemented
   *
   * @param context PipelineContext containing metadata and configuration
   * @return Try[ProcessingResult] with success/failure outcome
   */
  private def routeToProcessor(context: PipelineContext): Try[ProcessingResult] = {
    val metadata = context.metadata
    val fileLoadId = context.fileLoadId
    val industry = metadata.industry.toUpperCase
    val version = metadata.fileVersion
    
    // Pre-compute uppercase constants for stable case matching
    val commercialIndustry = CommercialConstants.BURUEAUTYPE_COMMERCIAL.toUpperCase
    val consumerIndustry = CommercialConstants.BURUEAUTYPE_CONSUMER.toUpperCase

    logger.info(s"[ROUTE] Validating industry='$industry', version='$version' for fileLoadId=$fileLoadId")
    
    Try {
      (industry, version) match {
        case (`commercialIndustry`, CommercialConstants.FILEVERSION_COMMERCIAL_V10) =>
          logger.info(s"[ROUTE] Route matched: ${CommercialConstants.BURUEAUTYPE_COMMERCIAL} ${CommercialConstants.FILEVERSION_COMMERCIAL_V10} → BTUDF10FileProcessor")
          val result = BTUDF10FileProcessor.processFile(context)
          handleProcessorResult(result, CommercialConstants.BURUEAUTYPE_COMMERCIAL, CommercialConstants.FILEVERSION_COMMERCIAL_V10, fileLoadId)
          
        case (`commercialIndustry`, CommercialConstants.FILEVERSION_COMMERCIAL_V30) =>
          logger.info(s"[ROUTE] Route matched: ${CommercialConstants.BURUEAUTYPE_COMMERCIAL} ${CommercialConstants.FILEVERSION_COMMERCIAL_V30} → BTUDF30FileProcessor")
          val result = BTUDF30FileProcessor.processFile(context)
          handleProcessorResult(result, CommercialConstants.BURUEAUTYPE_COMMERCIAL, CommercialConstants.FILEVERSION_COMMERCIAL_V30, fileLoadId)
          
        case (`consumerIndustry`, CommercialConstants.FILEVERSION_CONSUMER_V10) =>
          logger.warn(s"[ROUTE] ${CommercialConstants.BURUEAUTYPE_CONSUMER} processor (version=${CommercialConstants.FILEVERSION_CONSUMER_V10}) is not yet implemented. fileLoadId=$fileLoadId")
          throw new UnsupportedOperationException(s"Consumer file processor not yet implemented for version=${CommercialConstants.FILEVERSION_CONSUMER_V10}")
          
        case (ind, ver) =>
          val errorMsg = s"Unsupported combination: industry=$ind, version=$ver. Supported: ${CommercialConstants.BURUEAUTYPE_COMMERCIAL.toUpperCase} (${CommercialConstants.FILEVERSION_COMMERCIAL_V10}, ${CommercialConstants.FILEVERSION_COMMERCIAL_V30}), ${CommercialConstants.BURUEAUTYPE_CONSUMER} (pending implementation)"
          logger.error(s"[ROUTE] Route match failed: $errorMsg. fileLoadId=$fileLoadId")
          throw new IllegalArgumentException(errorMsg)
      }
    }
  }

  /**
   * Handle processor result with consistent logging
   * 
   * @param result Try[ProcessingResult] from processor
   * @param industry Industry type (for logging)
   * @param version File version (for logging)
   * @param fileLoadId File load ID (for logging)
   * @return ProcessingResult on success, throws exception on failure
   */
  private def handleProcessorResult(
    result: Try[ProcessingResult],
    industry: String,
    version: String,
    fileLoadId: Long
  ): ProcessingResult = {
    result match {
      case Success(processingResult) =>
        logger.info(s"[ROUTE] Processor executed successfully. industry=$industry, version=$version, fileLoadId=$fileLoadId, recordCount=${processingResult.recordCount}")
        processingResult
        
      case Failure(exception) =>
        logger.error(s"[ROUTE] Processor execution failed. industry=$industry, version=$version, fileLoadId=$fileLoadId, error=${exception.getMessage}", exception)
        throw exception
    }
  }
}
