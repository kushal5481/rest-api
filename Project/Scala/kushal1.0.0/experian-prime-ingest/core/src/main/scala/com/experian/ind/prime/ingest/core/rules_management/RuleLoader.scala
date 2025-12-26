package com.experian.ind.prime.ingest.core.rules_management

import com.experian.ind.prime.ingest.core.Util.JsonUtil
import com.experian.ind.prime.ingest.core.shared_models.Rule_Models.RuleModel

import scala.io.Source

object RuleLoader {

  def loadAllRules(path: String): List[RuleModel] = {
    try{

      println(s"[RuleLoader] Loading rules from path: $path")
      val source = Source.fromFile(path)
      val jsonStr = try source.mkString finally source.close()

      val rulesList = JsonUtil.fromJson[List[RuleModel]](jsonStr)
      println(s"[RuleLoader] Loaded ${rulesList.size} rules from path: $path")

      rulesList
    }catch{
      case ex: Exception =>
        println(s"Error loading rules from path: $path. Exception: ${ex.getMessage}")
        throw ex
    }
  }

}
