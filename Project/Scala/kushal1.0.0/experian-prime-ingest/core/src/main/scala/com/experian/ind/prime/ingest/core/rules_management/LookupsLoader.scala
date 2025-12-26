package com.experian.ind.prime.ingest.core.rules_management

import com.experian.ind.prime.ingest.core.Util.JsonUtil
import com.experian.ind.prime.ingest.core.shared_models.lookup_models.LookupModel

import java.io.File
import scala.io.Source

object LookupsLoader {


  /** Load All lookup JSONs and return Map[fileName -> List[LookupEntry]] */
  def loadAllLookups(folderPath: String): Map[String,List[LookupModel]] = {
    val folder = new File(folderPath)
    if(!folder.exists()){
      println(s"[WARN] Lookup folder $folderPath does not exist.")
      return Map.empty[String, List[LookupModel]]
    }

    val lookupFiles = folder.listFiles.filter(_.isFile).filter(_.getName.endsWith(".json"))

    lookupFiles.map { file =>
      val fileName = file.getName.replace(".json", "")
      val json = scala.io.Source.fromFile(file).mkString
      val data = JsonUtil.fromJson[List[LookupModel]](json)
      fileName -> data
    }.toMap
  }


}
