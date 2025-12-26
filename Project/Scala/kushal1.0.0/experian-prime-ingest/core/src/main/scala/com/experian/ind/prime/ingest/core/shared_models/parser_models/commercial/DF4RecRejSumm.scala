package com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial

/**
 * DataFrame-4 Record Reject Summary model
 */
case class DF4RecRejSumm(
  FileLoadID: Long,
  BatchID: Int,
  RecordID: String,
  IsFile_Reject: Boolean,
  IsRecord_Reject: Boolean,
  IsSegment_Reject: Boolean,
  IsField_Reject: Boolean
)
