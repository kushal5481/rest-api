package com.experian.ind.prime.ingest.core.shared_models.Rule_Models

case class RuleModel(
                      rule_id: String,
                      message: String,
                      status: String,

                      rule_type: String,
                      rule_category: String,
                      validation_scope: String,
                      severity: String,
                      priority: Int,

                      segments: List[String],
                      field_tag: List[String],

                      conditionType: String,
                      condition: String,

                      action: String,
                      actionModification: String,

                      expected_value_source: Option[String],
                      expected_values: List[String],

                      spec_date: String,

                      metricCodePass: String,
                      metricCodeFail: String
                    )
