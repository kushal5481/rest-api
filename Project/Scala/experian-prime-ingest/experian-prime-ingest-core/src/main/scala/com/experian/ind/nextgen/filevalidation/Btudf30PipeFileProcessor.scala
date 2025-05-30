package com.experian.ind.nextgen.filevalidation

import org.apache.spark.sql.SparkSession
import com.experian.ind.nextgen.model.FileConfig

object Btudf30PipeFileProcessor extends FileProcessorTrait {
    override def process(config: FileConfig, spark: SparkSession): Unit = {

    }
}