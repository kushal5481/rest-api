package com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial

/**
 * DataFrame Record model matching the output format
 */
case class DF1RawRecords(
  FileLoadID: Long,
  BatchID: Int,
  RecordID: String,
  Type_of_Line: String,
  RawRecord: String,
  RecordInsertionTime: String,
  Splitter_Status: String
)

/**
 * Record group representing a logical record (HD or BS with all child records)
 */
case class RecordGroup(
  groupType: String,  // "HEADER", "RECORD", "FOOTER"
  records: List[ParsedRecord]
)

/**
 * Processing context with IDs
 */
case class ProcessingContext(
  fileLoadId: Long,
  batchId: Int,
  var recordCounter: Int = 0
) {
  def nextRecordId(): String = {
    recordCounter += 1
    com.experian.ind.prime.ingest.core.Util.parser.commercial.IDGenerator.generateRecordId(recordCounter)
  }
}

/**
 * Result of file processing
 */
case class ProcessingResult(
  recordCount: Int,
  outputPath: String,
  success: Boolean,
  batchDataFrames: List[org.apache.spark.sql.DataFrame]
)
