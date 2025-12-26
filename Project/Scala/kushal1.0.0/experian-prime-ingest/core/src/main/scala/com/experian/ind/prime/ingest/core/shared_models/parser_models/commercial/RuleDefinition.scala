package com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial

/**
  * Typed model for commercial parser rules, mirroring the JSON schema
  * used in files like BTUDF10_FixedLength_Rules.json.
  */
final case class RuleDefinition(
    rule_id: String,
    rule_name: Option[String],
    rule_level: Option[String],
    rule_engine_type: Option[String],
    rule_type: Option[String],
    rule_category: Option[String],
    rule_status: Option[String],
    error_type: Option[String],
    severity: Option[String],
    rule_stage: Option[String],
    rule_priority: Option[Long],
    rule_action: Option[String],
    error_code: Option[String],
    error_message: Option[String],
    segment: Option[Seq[String]],
    field_tag: Option[Seq[String]],
    SQL_expression: Option[String],
    DSL_condition: Option[String],
    expected_value_source: Option[String],
    expected_values: Option[Seq[String]],
    spec_date: Option[String],
    metric_code: Option[String]
)
