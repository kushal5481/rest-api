package com.experian.ind.prime.ingest.core.parser.commercial.impl

import com.experian.ind.prime.ingest.core.Util.parser.commercial.DualLogger
import com.experian.ind.prime.ingest.core.parser.commercial.engine.{PipeDelimitedHTParser,FixedLengthHTParser,PipeDelimitedRecordParser,FixedLengthRecordParser}
import com.experian.ind.prime.ingest.core.parser.commercial.util.CommercialConstants
import com.experian.ind.prime.ingest.core.shared_models.PipelineContext
import org.apache.spark.sql.DataFrame

class FileParserImpl(context: PipelineContext) {

  private lazy val logger = DualLogger(getClass)
  
  // Extract format once at class level for reuse in both methods
  private lazy val fileFormat: String = context.fileFormatConfigModel.fileMetaData.format.toUpperCase
  private lazy val fileLoadId: Long = context.fileLoadId

  /**
   * Instantiate and invoke the appropriate file-level (header-trailer) parser
   * based on the configured file format
   *
   * @param sourceDF Input DataFrame (DF1)
   * @return List[DataFrame] containing DF3/DF4 if rules fail, or empty list if rules pass
   */
  def parsingWithHeaderTrailer(
    sourceDF: DataFrame
  ): List[DataFrame] = {
    logger.info(s"[IMPL] parsingWithHeaderTrailer: fileLoadId='$fileLoadId', format='$fileFormat'")
    
    try {
      val result = fileFormat match {
        case x if x == CommercialConstants.FORMAT_PIPEDELIMITED.toUpperCase =>
          logger.debug(s"[IMPL] Instantiating PipeDelimitedHTParser for fileLoadId='$fileLoadId'")
          val parser = new PipeDelimitedHTParser(context)
          parser.parseFile(sourceDF)
        case x if x == CommercialConstants.FORMAT_FIXEDLENGTH.toUpperCase =>
          logger.debug(s"[IMPL] Instantiating FixedLengthHTParser for fileLoadId='$fileLoadId'")
          val parser = new FixedLengthHTParser(context)
          parser.parseFile(sourceDF)
        case other =>
          throw new IllegalArgumentException(s"Unsupported file format: '$other' for fileLoadId='$fileLoadId'")
      }
      logger.info(s"[IMPL] parsingWithHeaderTrailer completed: fileLoadId='$fileLoadId', format='$fileFormat', resultCount=${result.size}")
      result
    } catch {
      case ex: Exception =>
        logger.error(s"[IMPL] Exception in parsingWithHeaderTrailer: fileLoadId='$fileLoadId', format='$fileFormat', error='${ex.getMessage}'", ex)
        throw ex
    }
  }

  /**
   * Instantiate and invoke the appropriate record-level parser
   * based on the configured file format
   *
   * @param sourceDF Input DataFrame (DF1)
   * @return DataFrame DF2 (parsed records)
   */
  def parsingWithRecord(
    sourceDF: DataFrame
  ): DataFrame = {
    logger.info(s"[IMPL] parsingWithRecord: fileLoadId='$fileLoadId', format='$fileFormat'")
    
    try {
      val result = fileFormat match {
        case x if x == CommercialConstants.FORMAT_PIPEDELIMITED.toUpperCase =>
          logger.debug(s"[IMPL] Instantiating PipeDelimitedRecordParser for fileLoadId='$fileLoadId'")
          val parser = new PipeDelimitedRecordParser(context)
          parser.parseFile(sourceDF)
        case x if x == CommercialConstants.FORMAT_FIXEDLENGTH.toUpperCase =>
          logger.debug(s"[IMPL] Instantiating FixedLengthRecordParser for fileLoadId='$fileLoadId'")
          val parser = new FixedLengthRecordParser(context)
          parser.parseFile(sourceDF)
        case other =>
          throw new IllegalArgumentException(s"Unsupported file format: '$other' for fileLoadId='$fileLoadId'")
      }
      logger.info(s"[IMPL] parsingWithRecord completed: fileLoadId='$fileLoadId', format='$fileFormat', rowCount=${result.count()}")
      result
    } catch {
      case ex: Exception =>
        logger.error(s"[IMPL] Exception in parsingWithRecord: fileLoadId='$fileLoadId', format='$fileFormat', error='${ex.getMessage}'", ex)
        throw ex
    }
  }
}
