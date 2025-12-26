package com.experian.ind.prime.ingest.core.shared_models.schema_models

case class SegmentExpectedValueModel(
                                    starts_with: String,
                                    MinRepeat: Int,
                                    MaxRepeat: Int,
                                    )