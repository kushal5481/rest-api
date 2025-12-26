package com.experian.ind.prime.ingest.core.shared_models.lookup_models

case class LookupModel(
                      code: String,
                      description: String,
                      isActive: Boolean,
                      PrefixMin: String,
                      PrefixMax: String,
                      Added_Modified_Comment: String
                      )
