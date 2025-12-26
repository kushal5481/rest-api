package com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial

/**
 * File format configuration model representing the JSON structure
 */
case class FileFormatConfig(
  fileMetaData: FileMetaData,
  fileStructure: FileStructure,
  recordStructure: RecordStructure,
  Segments: List[Segment]
)

/**
 * File metadata information
 */
case class FileMetaData(
  fileName: String,
  fileType: String,
  ucrfVersion: String,
  status: String,
  fileVersion: String,
  format: String,
  industry: String,
  description: String,
  isActive: Boolean
)

/**
 * File structure defining start and end segments
 */
case class FileStructure(
  startSegment: String,
  endSegment: String,
  Delimiter: String,
  ruleId: List[String]
)

/**
 * Record structure defining segment relationships
 */
case class RecordStructure(
  recordStartSegment: String,
  recordMandatorySegment: List[String],
  recordOptionalSegment: List[String],
  recordEndSegment: List[String],
  recordSegmentOrder: List[String],
  ruleId: List[String]
)

/**
 * Segment definition with fields
 */
case class Segment(
  Segment_Tag: String,
  Segment_Name: String,
  Segment_TypeofLine: String,
  Segment_IsMandatory: Boolean,
  Segment_Character_Type: String,
  Segment_Length_Type: String,
  Segment_Maximum_Length: Int,
  Segment_Occurrence: String,
  Segment_Priority: Int,
  Segment_Parent: Option[String],
  Segement_Delimiter_Count: Int,
  Segment_Previous: List[String],
  Segment_Immediate: List[String],
  Segment_Expected_Values: List[String],
  ruleId: List[String],
  Fields: List[Field]
)

/**
 * Field definition within a segment
 */
case class Field(
  Field_Index: Int,
  Field_Tag: String,
  Field_Name: String,
  Field_IsMandatory: Boolean,
  Field_Priority: Int,
  Field_Character_Type: String,
  Field_Length_Type: String,
  Field_Maximum_Length: Int,
  Field_Occurrence: String,
  Field_Expected_Values: List[String],
  Field_IsDate: Boolean,
  Field_Date_Format: String,
  Field_IsFiller: Boolean,
  ruleId: List[String]
)
