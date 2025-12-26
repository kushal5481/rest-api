package com.experian.ind.prime.ingest.core.splitter

import com.experian.ind.prime.ingest.core.shared_models.PipelineContext
import com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial.MetadataConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.monotonically_increasing_id

object ConsumerRecordSplitterService {

  def split(context: PipelineContext): DataFrame = {
    import  context.spark.implicits._

    val df = context.spark.read.text(context.metadata.inputPath+"/"+context.metadata.fileName)
      .withColumn("recordId", monotonically_increasing_id())
      .withColumnRenamed("value","raw_record")

    df
  }

}