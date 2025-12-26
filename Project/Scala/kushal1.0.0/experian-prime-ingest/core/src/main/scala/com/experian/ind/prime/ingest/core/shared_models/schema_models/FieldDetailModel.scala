package com.experian.ind.prime.ingest.core.shared_models.schema_models


case class FieldDetailModel(
                             Field_Tag: String,
                             Field_Index: Int,
                             Field_IsFiller: Boolean,
                             Field_Name: String,
                             Field_IsMandatory: Boolean,
                             Field_Character_Type: String,
                             Field_Length_Type: String,
                             Field_Maximum_Length: Int,
                             Field_Expected_Values: List[String],
                             Field_Expected_Values_Source: Option[List[String]],
                             Field_IsDateTime: Boolean,
                             Field_Date_Time_Format: Option[String]
                           )

