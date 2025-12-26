package com.experian.ind.prime.ingest.core.shared_models

import com.experian.ind.prime.ingest.core.shared_models.Metrics_model.MetricModel
import com.experian.ind.prime.ingest.core.shared_models.Rule_Models.RuleModel
import com.experian.ind.prime.ingest.core.shared_models.lookup_models.LookupModel
import com.experian.ind.prime.ingest.core.shared_models.schema_models.FileMainConfigModel

case class ValidationContextModel(
                                 schema: FileMainConfigModel,
                                 lookups: Map[String, List[LookupModel]],
                                 metrics: List[MetricModel],
                                  rules: List[RuleModel]
                                 )
