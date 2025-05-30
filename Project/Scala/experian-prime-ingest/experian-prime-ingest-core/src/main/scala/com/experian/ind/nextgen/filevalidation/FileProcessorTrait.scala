package com.experian.ind.nextgen.filevalidation

import org.apache.spark.sql.SparkSession
import com.experian.ind.nextgen.model.FileConfig

trait FileProcessorTrait {
    def process(config: FileConfig, spark: SparkSession): Unit
}