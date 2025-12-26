package com.experian.ind.prime.ingest.core.shared_models.schema_models

case class SegmentDetailModel(
                               Segment_Tag: String,
                               Segment_Name: String,
                               IsMandatorySegment: Boolean,
                               Segment_Character_Type: String,
                               Segment_Length_Type: String,
                               segment_occurrence: String,
                               Segment_Maximum_Length: Option[Int],
                               Segment_Expected_Values: Option[List[SegmentExpectedValueModel]],
                               Field_Details: List[FieldDetailModel]
                             )

