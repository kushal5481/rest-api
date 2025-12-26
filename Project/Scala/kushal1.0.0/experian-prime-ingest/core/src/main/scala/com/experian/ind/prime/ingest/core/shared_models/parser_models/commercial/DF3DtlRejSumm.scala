package com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial

/**
 * DataFrame-3 Detailed Reject Summary model
 */
case class DF3DtlRejSumm(
  FileLoadID: Long,
  BatchID: Int,
  RecordID: String,
  Segment_Tag: String,
  Segment_Repeat_Id: String,
  Field_Tag: String,
  Field_Value: String,
  Error_Stage: String,
  //Error_Type: String,
  Severity: String,
  Error_Code: String,
  Error_Message: String,
  Rule_Id: String,
  Metric_Code: String,
  Reject_Type: String,
  Created_At: String
)
