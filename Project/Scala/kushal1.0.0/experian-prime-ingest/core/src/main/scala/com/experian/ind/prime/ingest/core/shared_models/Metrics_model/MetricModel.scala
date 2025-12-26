package com.experian.ind.prime.ingest.core.shared_models.Metrics_model

case class MetricModel(
                        metricCode: String,
                        MetricTriggerService: String,
                        metricName: String,
                        metricDescription: String,
                        metricParentCode: String
                      )
