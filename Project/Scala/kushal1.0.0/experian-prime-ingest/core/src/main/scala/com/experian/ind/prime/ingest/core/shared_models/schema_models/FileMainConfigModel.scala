package com.experian.ind.prime.ingest.core.shared_models.schema_models

case class FileMainConfigModel(
                            filename: String,
                            version: String,
                            format: String,
                            delimiter:String,
                            inputFilePath: String,
                            outputFilePath: String,
                            isActive: Boolean,
                            Record_Details: List[RecordDetailModel],
                            Segments_Details: List[SegmentDetailModel]
                          )