package com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial

/**
 * File configuration entry from file-config.json
 */
case class FileConfig(
  fileType: String,
  version: String,
  format: String,
  industry: String,
  jsonTemplate: String,
  templateLocation: String,
  ruleLocation: String
)
