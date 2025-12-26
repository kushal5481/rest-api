package com.experian.ind.prime.ingest.core.splitter.commercial.util

import com.experian.ind.prime.ingest.core.Util.parser.commercial.{DualLogger, Stopwatch}
import com.experian.ind.prime.ingest.core.parser.commercial.util.CommercialConstants
import com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial._
import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
 * Parser for pipe-delimited UCRF files
 */
class PipeDelimitedSplitter(fileFormatConfigModel: FileFormatConfig) {
  
  private lazy val logger = DualLogger(getClass)
  private val formatConfig: FileFormatConfig = fileFormatConfigModel
  private val SEPARATOR = "=" * 80
  // Derive delimiter from schema configuration (no hardcoding)
  private lazy val delimiter: String = Option(formatConfig.fileStructure.Delimiter)
    .map(_.trim)
    .filter(_.nonEmpty)
    .getOrElse("|")
  
  /**
   * Parse the pipe-delimited file
   * 
   * @param filePath Path to the pipe-delimited input file
   * @return List of parsed records with segment tags and field maps
   */
  def splitFile(filePath: String): List[ParsedRecord] = {
    val stepStopwatch = Stopwatch.createStarted()
    logger.info(SEPARATOR)
    logger.info(s"[STEP 1] Starting pipe-delimited file parsing. filePath=$filePath, delimiter='$delimiter'")
    
    var source: Source = null
    try {
      source = Source.fromFile(filePath)
      val lines = source.getLines().toList
      logger.info(s"[STEP 1] File loaded: totalLines=${lines.size}")
      
      logger.info(s"[STEP 2] Parsing lines into records...")
      val splittedRecords = lines.zipWithIndex.flatMap { case (line, index) =>
        splitLine(line, index + 1)
      }
      
      stepStopwatch.stop()
      logger.info(s"[STEP 3] Parsing completed successfully. parsedRecords=${splittedRecords.size}, totalLines=${lines.size}, elapsedTime=${stepStopwatch.elapsed()}")
      logger.info(SEPARATOR)
      splittedRecords
    } catch {
      case ex: Exception =>
        logger.error(s"[STEP 1] Exception in splitFile. filePath=$filePath, delimiter='$delimiter': ${ex.getMessage}", ex)
        List.empty
    } finally {
      if (source != null) source.close()
      logger.info(s"[CLEANUP] File resource closed. filePath=$filePath")
    }
  }
  
  /**
   * Parse a single line based on segment identifier
   * Splits line by delimiter and extracts fields from configuration
   * 
   * @param line The line to parse
   * @param lineNumber The line number (for logging)
   * @return Option[ParsedRecord] - Some if valid, None if parse error or empty line
   */
  def splitLine(line: String, lineNumber: Int): Option[ParsedRecord] = {
    if (line.trim.isEmpty) {
      logger.debug(s"[LINE] Skipping empty line=$lineNumber")
      return None
    }
    
    Try {
      // Split by delimiter from config (escape for regex split)
      val escapedDelimiter = java.util.regex.Pattern.quote(delimiter)
      val fields = line.split(escapedDelimiter, -1)
      logger.debug(s"[LINE] Split line=$lineNumber into ${fields.length} fields")
      
      if (fields.isEmpty) {
        logger.warn(s"[LINE] No fields found at line=$lineNumber")
        return None
      }
      
      // Extract segment tag (first field)
      val segmentTag = fields(0).trim
      logger.debug(s"[LINE] Extracted segment tag='$segmentTag' from line=$lineNumber")
      
      // Find segment configuration
      val segmentConfig = formatConfig.Segments.find(_.Segment_Tag == segmentTag)
      segmentConfig match {
        case Some(segment) =>
          logger.debug(s"[LINE] Segment config found for tag='$segmentTag'. lineNumber=$lineNumber")
          val fieldMap = extractFieldsFromDelimited(fields, segment)
          logger.debug(s"[LINE] Extracted ${fieldMap.size} fields from line=$lineNumber")
          Some(ParsedRecord(segmentTag, lineNumber, line, fieldMap))
        case None =>
          // If unknown, treat as Record (BS segment)
          val bsConfigOpt = formatConfig.Segments.find(_.Segment_Tag == CommercialConstants.BORROWER_TAG)
          bsConfigOpt match {
            case Some(bsConfig) =>
              logger.warn(s"[LINE] Unknown segment tag='$segmentTag' at line=$lineNumber. Treating as Record (BS segment).")
              val fieldMap = extractFieldsFromDelimited(fields, bsConfig)
              Some(ParsedRecord(CommercialConstants.BORROWER_TAG, lineNumber, line, fieldMap))
            case None =>
              logger.error(s"[LINE] Unknown segment tag='$segmentTag' at line=$lineNumber and no BS segment config found. Skipping line.")
              None
          }
      }
    } match {
      case Success(result) => result
      case Failure(exception) =>
        logger.error(s"[LINE] Exception parsing line=$lineNumber, segmentTag='${if(line.nonEmpty) line.split(java.util.regex.Pattern.quote(delimiter))(0).trim else "<empty>"}': ${exception.getMessage}", exception)
        None
    }
  }
  
  /**
   * Extract fields from pipe-delimited values based on segment configuration
   * Maps field values to field names from segment config
   * 
   * @param values Array of pipe-delimited values from a line
   * @param segment Segment configuration with field definitions
   * @return Map of field name to field value
   */
  private def extractFieldsFromDelimited(values: Array[String], segment: Segment): Map[String, String] = {
    logger.debug(s"[EXTRACT] Extracting fields for segment='${segment.Segment_Tag}' from ${values.length} values")
    
    val fieldMap = segment.Fields.zipWithIndex.map { case (field, index) =>
      val value = if (index < values.length) values(index).trim else ""

      val fieldKey = if (field.Field_Tag.nonEmpty) {
        s"${segment.Segment_Tag}_${field.Field_Tag}_${field.Field_Name.replaceAll("[^a-zA-Z0-9]", "_")}"
      } else {
        s"${segment.Segment_Tag}_${field.Field_Name.replaceAll("[^a-zA-Z0-9]", "_")}"
      }

      fieldKey -> value
    }.toMap
    logger.debug(s"[EXTRACT] Extracted ${fieldMap.size} fields from segment values")

    // Dynamically include segment-level configuration without hardcoding JSON keys
    // Invoke case class getters reflectively and stringify values safely
    val segmentConfigMap: Map[String, String] = segment.getClass.getDeclaredMethods
      .filter { m =>
        m.getParameterCount == 0 &&
        m.getReturnType != classOf[Unit] &&
        m.getName != "Fields" &&
        !m.getName.startsWith("copy") &&
        !m.getName.startsWith("product")
      }
      .map { m =>
        val raw = try m.invoke(segment) catch { case _: Throwable => null }
        val str = raw match {
          case null => ""
          case opt: Option[_] => opt.getOrElse("").toString
          case l: List[_] => l.mkString(",")
          case other => other.toString
        }
        m.getName -> str
      }
      .toMap
    logger.debug(s"[EXTRACT] Added ${segmentConfigMap.size} segment config fields")

    fieldMap ++ segmentConfigMap
  }

  /**
   * Parse file into hierarchical structure with validation
   * Validates header/trailer presence and groups records by borrower
   * 
   * @param filePath Path to the pipe-delimited input file
   * @return FileParseResult with hierarchical record structure
   */
  def splitFileHierarchical(filePath: String): FileParseResult = {
    logger.info(s"[HIERARCHICAL] Starting hierarchical file parsing. filePath=$filePath")
    val allRecords = splitFile(filePath)
    logger.info(s"[HIERARCHICAL] Parsed ${allRecords.size} total records")
    
    logger.info(s"[HIERARCHICAL] Validating header segment...")
    val header = allRecords.find(_.segmentTag == CommercialConstants.HEADER_TAG).getOrElse({
      logger.error(s"[HIERARCHICAL] Header segment (HD) not found in ${allRecords.size} records")
      throw new IllegalStateException("Header segment (HD) not found")
    })
    logger.info(s"[HIERARCHICAL] Header segment (HD) found successfully")

    logger.info(s"[HIERARCHICAL] Validating trailer segment...")
    val trailer = allRecords.find(_.segmentTag == CommercialConstants.FILECLOSURE_TAG).getOrElse({
      logger.error(s"[HIERARCHICAL] Trailer segment (TS) not found in ${allRecords.size} records")
      throw new IllegalStateException("Trailer segment (TS) not found")
    })
    logger.info(s"[HIERARCHICAL] Trailer segment (TS) found successfully")

    logger.info(s"[HIERARCHICAL] Grouping ${allRecords.size} records by borrower...")
    val borrowers = groupByBorrower(allRecords)
    logger.info(s"[HIERARCHICAL] Grouped into ${borrowers.size} borrower records")

    FileParseResult(
      header = header,
      records = borrowers,
      footer = trailer,
      totalRecords = allRecords.size,
      errors = List.empty
    )
  }

  /**
   * Group records by borrower (BS segment) with hierarchical structure
   * Creates BorrowerRecord hierarchy: Borrower -> Relationships -> Credit Facilities -> Guarantors/Security/Dishonor
   * 
   * @param records All parsed records from file
   * @return List of BorrowerRecord with hierarchical structure
   */
  private def groupByBorrower(records: List[ParsedRecord]): List[BorrowerRecord] = {
    logger.info(s"[GROUP] Starting borrower grouping for ${records.size} records")
    val borrowerRecords = scala.collection.mutable.ListBuffer[BorrowerRecord]()
    var currentBorrower: Option[ParsedRecord] = None
    var currentRelationships = scala.collection.mutable.ListBuffer[ParsedRecord]()
    var currentCreditFacilities = scala.collection.mutable.ListBuffer[CreditFacilityRecord]()
    var currentCreditFacility: Option[ParsedRecord] = None
    var currentGuarantors = scala.collection.mutable.ListBuffer[ParsedRecord]()

    var currentASList = scala.collection.mutable.ListBuffer[ParsedRecord]()
    var currentSSList = scala.collection.mutable.ListBuffer[ParsedRecord]()
    var currentCDList = scala.collection.mutable.ListBuffer[ParsedRecord]()

    records.foreach { record =>
      record.segmentTag match {
        case CommercialConstants.HEADER_TAG | CommercialConstants.FILECLOSURE_TAG =>
          // Skip header and trailer

        case CommercialConstants.BORROWER_TAG =>
          logger.debug(s"[GROUP] BS segment encountered. currentBorrower=${currentBorrower.isDefined}")
          // Save previous borrower if exists
          if (currentBorrower.isDefined) {
            if (currentCreditFacility.isDefined) {
              val lastCreditFacility = currentCreditFacility.get
              currentCreditFacilities += CreditFacilityRecord(
                lastCreditFacility,
                currentGuarantors.toList,
                currentSSList.toList,
                currentCDList.toList
              )
            }
            borrowerRecords += BorrowerRecord(
              records.find(_.segmentTag == CommercialConstants.HEADER_TAG).get,
              currentBorrower.get,
              currentRelationships.toList ++ currentASList.toList,
              Nil,
              currentCreditFacilities.toList,
              None
            )
          }

          currentBorrower = Some(record)
          currentRelationships.clear()
          currentCreditFacilities.clear()
          currentCreditFacility = None
          currentGuarantors.clear()
          currentASList.clear()
          currentSSList.clear()
          currentCDList.clear()

        case CommercialConstants.RELATIONSHIP_TAG =>
          currentRelationships += record

        case CommercialConstants.CREDITFACILITY_TAG =>
          if (currentCreditFacility.isDefined) {
            val lastCreditFacility = currentCreditFacility.get
            currentCreditFacilities += CreditFacilityRecord(
              lastCreditFacility,
              currentGuarantors.toList,
              currentSSList.toList,
              currentCDList.toList
            )
          }
          currentCreditFacility = Some(record)
          currentGuarantors.clear()
          currentSSList.clear()
          currentCDList.clear()

        case CommercialConstants.GUARANTOR_TAG =>
          currentGuarantors += record

        case CommercialConstants.ADDRESS_TAG =>
          currentASList += record

        case CommercialConstants.SECURITY_TAG =>
          currentSSList += record

        case CommercialConstants.DISHONOURCHEQUE_TAG =>
          currentCDList += record

        case _ =>
          logger.warn(s"Unexpected segment: ${record.segmentTag}")
      }
    }

    if (currentBorrower.isDefined) {
      logger.debug(s"[GROUP] Saving final borrower record")
      if (currentCreditFacility.isDefined) {
        val lastCreditFacility = currentCreditFacility.get
        currentCreditFacilities += CreditFacilityRecord(
          lastCreditFacility,
          currentGuarantors.toList,
          currentSSList.toList,
          currentCDList.toList
        )
      }
      borrowerRecords += BorrowerRecord(
        records.find(_.segmentTag == CommercialConstants.HEADER_TAG).get,
        currentBorrower.get,
        currentRelationships.toList ++ currentASList.toList,
        Nil,
        currentCreditFacilities.toList,
        None
      )
    }

    val result = borrowerRecords.toList
    logger.info(s"[GROUP] Borrower grouping completed. totalBorrowers=${result.size}")
    result
  }
}
