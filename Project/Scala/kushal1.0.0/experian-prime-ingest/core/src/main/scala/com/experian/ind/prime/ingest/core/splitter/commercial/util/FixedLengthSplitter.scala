package com.experian.ind.prime.ingest.core.splitter.commercial.util

import com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial._
import com.experian.ind.prime.ingest.core.Util.parser.commercial.{DualLogger, Stopwatch}
import com.experian.ind.prime.ingest.core.parser.commercial.util.CommercialConstants

import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
 * Parser for fixed-length UCRF files
 */
class FixedLengthSplitter(fileFormatConfigModel: FileFormatConfig) {
  
  private lazy val logger = DualLogger(getClass)
  private val formatConfig: FileFormatConfig = fileFormatConfigModel
  
  private val SEPARATOR = "=" * 80
  
  /**
   * Parse the fixed-length file
   * 
   * @param filePath Path to the fixed-length input file
   * @return List of parsed records with segment tags and field maps
   */
  def splitFile(filePath: String): List[ParsedRecord] = {
    val stepStopwatch = Stopwatch.createStarted()
    logger.info(SEPARATOR)
    logger.info(s"[STEP 1] Starting fixed-length file parsing. filePath=$filePath")
    
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
        logger.error(s"[STEP 1] Exception in splitFile. filePath=$filePath: ${ex.getMessage}", ex)
        List.empty
    } finally {
      if (source != null) source.close()
      logger.info(s"[CLEANUP] File resource closed. filePath=$filePath")
    }
  }
  
  /**
   * Parse a single line based on segment identifier
   * Extracts segment tag and field values from fixed-position format
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
      // Extract segment tag (first 2 characters)
      val segmentTag = line.substring(0, Math.min(2, line.length))
      logger.debug(s"[LINE] Extracted segment tag='$segmentTag' from line=$lineNumber")

      // Find segment configuration
      val segmentConfig = formatConfig.Segments.find(_.Segment_Tag == segmentTag)
      segmentConfig match {
        case Some(segment) =>
          logger.debug(s"[LINE] Segment config found for tag='$segmentTag'. lineNumber=$lineNumber")
          val fields = extractFields(line, segment)
          logger.debug(s"[LINE] Extracted ${fields.size} fields from line=$lineNumber")
          Some(ParsedRecord(segmentTag, lineNumber, line, fields))
        case None =>
          // If unknown, treat as Record (BS segment)
          val bsConfigOpt = formatConfig.Segments.find(_.Segment_Tag == CommercialConstants.BORROWER_TAG)
          bsConfigOpt match {
            case Some(bsConfig) =>
              logger.warn(s"[LINE] Unknown segment tag='$segmentTag' at line=$lineNumber. Treating as Record (BS segment).")
              val fields = extractFields(line, bsConfig)
              Some(ParsedRecord(CommercialConstants.BORROWER_TAG, lineNumber, line, fields))
            case None =>
              logger.error(s"[LINE] Unknown segment tag='$segmentTag' at line=$lineNumber and no BS segment config found. Skipping line.")
              None
          }
      }
    } match {
      case Success(result) => result
      case Failure(exception) =>
        logger.error(s"[LINE] Exception parsing line=$lineNumber, segmentTag='${if(line.length >= 2) line.substring(0,2) else "<empty>"}': ${exception.getMessage}", exception)
        None
    }
  }
  
  /**
   * Extract fields from a line based on segment configuration
   */
  private def extractFields(line: String, segment: Segment): Map[String, String] = {
    var currentPosition = 0
    val fieldMap = segment.Fields.map { field =>
      val startPos = currentPosition
      val endPos = Math.min(currentPosition + field.Field_Maximum_Length, line.length)
      val value = if (endPos <= line.length) {
        line.substring(startPos, endPos).trim
      } else {
        ""
      }
      currentPosition = endPos
      // Create field key
      val fieldKey = if (field.Field_Tag.nonEmpty) {
        s"${segment.Segment_Tag}_${field.Field_Tag}_${field.Field_Name.replaceAll("[^a-zA-Z0-9]", "_")}"
      } else {
        s"${segment.Segment_Tag}_${field.Field_Name.replaceAll("[^a-zA-Z0-9]", "_")}"
      }
      fieldKey -> value
    }.toMap
    // Scala 2.12 compatible: extract all segment config keys except 'Fields'
    val fieldNames = segment.getClass.getDeclaredFields.map(_.getName).filter(_ != "Fields")
    val segmentConfigMap: Map[String, String] = fieldNames.zipWithIndex.map { case (key, idx) =>
      val value = segment.productElement(idx)
      key -> (value match {
        case Some(v) => v.toString
        case l: List[_] => l.mkString(",")
        case v => v.toString
      })
    }.toMap
    fieldMap ++ segmentConfigMap
  }
  
  /**
   * Parse file into hierarchical structure with validation
   * Validates header/trailer presence and groups records by borrower
   * 
   * @param filePath Path to the fixed-length input file
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
    
    // Group records by borrower
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

    // AS segments are children of BS
    var currentASList = scala.collection.mutable.ListBuffer[ParsedRecord]()
    // SS and CD segments are children of CR
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
              // Attach SS and CD to previous CR
              val lastCreditFacility = currentCreditFacility.get
              val crChildren = currentSSList.toList ++ currentCDList.toList
              currentCreditFacilities += CreditFacilityRecord(
                lastCreditFacility,
                currentGuarantors.toList,
                currentSSList.toList,
                currentCDList.toList
              )
            }
            // Attach AS to borrower
            borrowerRecords += BorrowerRecord(
              records.find(_.segmentTag == CommercialConstants.HEADER_TAG).get,
              currentBorrower.get,
              currentRelationships.toList ++ currentASList.toList,
              Nil,
              currentCreditFacilities.toList,
              None
            )
          }

          // Start new borrower
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
          // Save previous credit facility if exists
          if (currentCreditFacility.isDefined) {
            val lastCreditFacility = currentCreditFacility.get
            val crChildren = currentSSList.toList ++ currentCDList.toList
            currentCreditFacilities += CreditFacilityRecord(
              lastCreditFacility,
              currentGuarantors.toList,
              currentSSList.toList,
              currentCDList.toList
            )
          }
          // Start new credit facility
          currentCreditFacility = Some(record)
          currentGuarantors.clear()
          currentSSList.clear()
          currentCDList.clear()

        case CommercialConstants.GUARANTOR_TAG =>
          currentGuarantors += record

        case CommercialConstants.ADDRESS_TAG =>
          // AS segment: child of BS
          currentASList += record

        case CommercialConstants.SECURITY_TAG =>
          // SS segment: child of CR
          currentSSList += record

        case CommercialConstants.DISHONOURCHEQUE_TAG =>
          // CD segment: child of CR
          currentCDList += record

        case _ =>
          logger.warn(s"Unexpected segment: ${record.segmentTag}")
      }
    }

    // Save last borrower
    if (currentBorrower.isDefined) {
      logger.debug(s"[GROUP] Saving final borrower record")
      if (currentCreditFacility.isDefined) {
        val lastCreditFacility = currentCreditFacility.get
        val crChildren = currentSSList.toList ++ currentCDList.toList
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
