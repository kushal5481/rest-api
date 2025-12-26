package com.experian.ind.prime.ingest.core.shared_models

import com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial.MetadataConfig
import com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial.{FileFormatConfig, ParserConfig, FileConfig}
import org.apache.spark.sql.SparkSession

case class PipelineContext (
                           spark: SparkSession,
                           metadata: MetadataConfig,
                           parserConfigModel: ParserConfig,
                           fileFormatConfigModel: FileFormatConfig,
                           fileConfigModel: FileConfig,
                           fileLoadId: Long
                           )
