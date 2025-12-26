package com.experian.ind.prime.ingest.core.validation_engine

import com.experian.ind.prime.ingest.core.Util.JsonUtil
import com.experian.ind.prime.ingest.core.rules_management.{LookupsLoader, MetricsLoader, RuleLoader, SchemaLoader}
import com.experian.ind.prime.ingest.core.shared_models.{PipelineContext, ValidationContextModel}
import org.apache.spark.sql.DataFrame

object ConsumerValidationEngine {


  def validate(parsedDF: DataFrame, context: PipelineContext): Unit = {

    val spark = context.spark

    /** Loading Schema */
    println("====== STEP:1 Load Schema ======")
    val schmeapath = "properties/schemas/consumer/2.0/version1/file-config.json"
    val schema = SchemaLoader.loadSchema(schmeapath)
    println("====== Schema Loaded Successfully ======")
    println(JsonUtil.toJson(schema))

    /** Loading Lookups */
    println("====== STEP:2 Load Lookups ======")
    val lookupsPath = "properties/lookups/consumer/2.0/version1/"
    val lookupsMap = LookupsLoader.loadAllLookups(lookupsPath)
    println("====== Lookups Loaded Successfully ======")
    lookupsMap.foreach { case (name, values) =>
      println(s"Lookup: $name -> ${values.size} entries")
      values.foreach(v => println(s"  ${v.code} - ${v.description}"))
    }

    /** Loading Metrics */
    println("====== STEP:3 Load Metrics ======")
    val metricsPath = "properties/metrics/consumer_metrics.json"
    val metricsList = MetricsLoader.loadAllMetrics(metricsPath)
    println("====== Metrics Loaded Successfully ======")

    /** Loading Rules */
    println("====== STEP:4 Load Rules ======")
    val rulesPath = "properties/rules/validation-engine-rules/consumer/2.0/version1/ctudf20_rules.json"
    val rules = RuleLoader.loadAllRules(rulesPath)
    rules.foreach(r => println(s"Rule ID: ${r.rule_id}, Type: ${r.rule_type}, Condition: ${r.condition},) Action: ${r.action}"))
    println("====== Rules Loaded Successfully ======")

    /** BroadCasting Schema, Lookups, Metrics, Rules */
    println("====== STEP:5 Broadcasting Schema/Lookups/Metrics/Rules ======")
    val schemaObj = schema.head
    val ctx = ValidationContextModel(
      schemaObj,
      lookupsMap,
      metricsList,
      rules
    )
    val ctxBC = spark.sparkContext.broadcast(ctx)
    println("====== Broadcasting Completed Successfully ======")





  }

}
