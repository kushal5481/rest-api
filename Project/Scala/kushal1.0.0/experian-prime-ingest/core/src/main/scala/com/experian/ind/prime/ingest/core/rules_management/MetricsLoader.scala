package com.experian.ind.prime.ingest.core.rules_management

import com.experian.ind.prime.ingest.core.Util.JsonUtil
import com.experian.ind.prime.ingest.core.shared_models.Metrics_model.MetricModel

import scala.io.Source

object MetricsLoader {
  def loadAllMetrics(path: String): List[MetricModel] = {
   try{
     println(s"Loading metrics from path: $path")
     val source = Source.fromFile(path)
     val jsonStr = try source.mkString finally source.close()

     val metricList = JsonUtil.fromJson[List[MetricModel]](jsonStr)
     println(s"Loaded ${metricList.size} metrics from $path")
     metricList
   }
    catch{
      case ex: Exception=>{
        throw new Exception(s"Error loading metrics from path: $path", ex)
      }
    }
  }
}