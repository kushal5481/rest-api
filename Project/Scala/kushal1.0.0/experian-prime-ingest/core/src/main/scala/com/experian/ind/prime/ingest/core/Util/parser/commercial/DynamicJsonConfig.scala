package com.experian.ind.prime.ingest.core.Util.parser.commercial

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.io.Source

object DynamicJsonConfig {
  private val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)
  private var configJson: JsonNode = _

  def loadConfig(path: String): Unit = {
    val source = Source.fromFile(path)
    try {
      configJson = objectMapper.readTree(source.mkString)
    } finally {
      source.close()
    }
  }

  def get(key: String): Option[JsonNode] = {
    if (configJson == null) None else Option(configJson.get(key))
  }

  def getRaw: JsonNode = configJson
}
