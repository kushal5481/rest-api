package com.experian.ind.prime.ingest.core.orchestrator

import com.experian.ind.prime.ingest.core.shared_models.PipelineContext
import com.experian.ind.prime.ingest.core.splitter.ConsumerRecordSplitterService
import com.experian.ind.prime.ingest.core.splitter.CommercialRecordSplitterService
import com.experian.ind.prime.ingest.core.validation_engine.ConsumerValidationEngine
import com.experian.ind.prime.ingest.core.parser.commercial.CommercialFileParserService
import org.apache.commons.lang3.Validate

import java.nio.file.Paths

object Orchestrator {

  def start(context: PipelineContext): Unit = {
    println("Orchestrator started.")

    // Fetch bureau type from metadata (cached in context)
    val bureauType = context.metadata.industry
    println(s"Detected Bureau Type: $bureauType")

    bureauType match {
      case "Commercial" =>
        println("Starting Commercial file parsing...")
        CommercialFileParserService.startFileParsing(context)

      case "Consumer" =>
        println(" ===== LOADING PARSED DATA FRAME FROM CSV ===== ")
        val parsedDF = context.spark.read
          .option("header","true")
          .option("inferSchema","false")
          .option("quote","\"")
          .option("escape","\"")
          .csv("properties/sample_data/Ctudf20_Hybrid_DataFrame.csv")
        println(" ========== PARSED DATA FRAME SAMPLE ========== ")
        parsedDF.show(150,false)
        val validatedDF = ConsumerValidationEngine.validate(parsedDF, context)

      case other =>
        println(s"Unsupported bureau type: $other")
        throw new IllegalArgumentException(s"Unsupported bureau type: $other")
    }
    println("Pipeline Execution Completed.")
  }
}
