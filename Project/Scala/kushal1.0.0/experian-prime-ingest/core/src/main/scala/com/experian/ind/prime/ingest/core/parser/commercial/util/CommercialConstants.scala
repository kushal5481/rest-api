package com.experian.ind.prime.ingest.core.parser.commercial.util

/**
 * Centralized constants for parser and rule engine outputs.
 */
object CommercialConstants {
  // Error stages
  val ERROR_STAGE = "PARSER"

  // Error types
  val ERROR_TYPE = "FATAL"

  // Severity levels
  val SEVERITY = "CRITICAL"

  // Reject flags
  val TRUE = true
  val FALSE = false

val BURUEAUTYPE_COMMERCIAL = "Commercial"
val BURUEAUTYPE_CONSUMER = "Consumer"
val FILEVERSION_COMMERCIAL_V10 = "1.0"
val FILEVERSION_COMMERCIAL_V30 = "3.0"
val FILEVERSION_CONSUMER_V10 = "1.0"
  val HEADER = "Header"
  val FOOTER = "Footer"
  val RECORD = "Record"
  val ERRORTYPE_FATAL = "FATAL"
  val RULELEVEL_FILE = "FILE"
  val RULELEVEL_RECORD = "RECORD"
  val RULEACTION_REJECT = "REJECT"
  val FORMAT_FIXEDLENGTH = "FixedLength"
  val FORMAT_PIPEDELIMITED = "PipeDelimited"
  val DATETIMEFORMAT = "dd-MM-yyyy HH:mm:ss"
  val SEGEMENT_MANDATORY = "Segment_IsMandatory"
  val SEGMENT_LENGTH = "Segment_Maximum_Length"
  val SEGMENT_OCCURRENCE = "Segment_Occurrence"
  val SEGEMENT_DELIMITER_COUNT = "Segement_Delimiter_Count"
  val TYPEOFLINE = "Type_of_Line"
  val FILELOADID = "FileLoadID"
  val BATCHID = "BatchID"
  val RECORDID = "RecordID"
  val RULE_LEVEL_FILE = "FILE"
  val RULE_LEVEL_RECORD = "RECORD"
  val RULE_LEVEL_SEGEMENT = "SEGMENT"
  val RULE_LEVEL_FIELD = "FIELD"
  val SEGMENT = "segment"
  val FIELDTAG = "field_tag"
  val RULESTATUS = "rule_status"
  val RULEENGINETYPE = "rule_engine_type"
  val RULEID = "rule_id"
  val RULEPRIORITY = "rule_priority"
  val RULESTATUS_ACTIVE = "active"
  val RULEENGINETYPE_SQL = "sql"
  val RULEENGINETYPE_DSL = "dsl"
  val DELIMITERSIGN = "|"
  val UNDERSCORESIGN = "_"
  val DELIMITERSIGNINTEXT = "pipe"
  val HEADER_TAG = "HD"
  val BORROWER_TAG = "BS"
  val ADDRESS_TAG = "AS"
  val RELATIONSHIP_TAG = "RS"
  val CREDITFACILITY_TAG = "CR"
  val GUARANTOR_TAG = "GS"
  val SECURITY_TAG = "SS"
  val DISHONOURCHEQUE_TAG = "CD"
  val FILECLOSURE_TAG = "TS"
}
