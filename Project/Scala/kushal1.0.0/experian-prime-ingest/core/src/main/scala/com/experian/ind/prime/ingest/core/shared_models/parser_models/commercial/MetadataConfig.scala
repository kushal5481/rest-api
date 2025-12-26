package com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial

/**
 * Metadata configuration from properties file
 */
case class MetadataConfig(
  industry: String,
  fileVersion: String,
  fileFormat: String,
  fileName: String,
  inputPath: String
)
