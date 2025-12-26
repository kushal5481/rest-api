package com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial

/**
 * Parsed record from input file
 */
case class ParsedRecord(
  segmentTag: String,
  lineNumber: Int,
  rawLine: String,
  Fields: Map[String, String]
)

/**
 * Hierarchical structure for borrower records
 */
case class BorrowerRecord(
  header: ParsedRecord,
  record: ParsedRecord,
  addresses: List[ParsedRecord],
  relationships: List[ParsedRecord],
  creditFacilities: List[CreditFacilityRecord],
  footer: Option[ParsedRecord]
)

/**
 * Credit facility with associated guarantor, security, and dishonor records
 */
case class CreditFacilityRecord(
  creditFacility: ParsedRecord,
  guarantors: List[ParsedRecord],
  security: List[ParsedRecord],
  dishonor: List[ParsedRecord]
)

/**
 * File parsing result
 */
case class FileParseResult(
  header: ParsedRecord,
  records: List[BorrowerRecord],
  footer: ParsedRecord,
  totalRecords: Int,
  errors: List[ParseError]
)

/**
 * Parse error information
 */
case class ParseError(
  lineNumber: Int,
  errorMessage: String,
  segmentTag: String,
  rawLine: String
)