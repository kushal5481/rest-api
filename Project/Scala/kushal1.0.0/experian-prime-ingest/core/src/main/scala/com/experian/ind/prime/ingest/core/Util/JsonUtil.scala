package com.experian.ind.prime.ingest.core.Util

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object JsonUtil {

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def fromJson[T](json:String)(implicit m: Manifest[T]): T = {
    mapper.readValue[T](json, m.runtimeClass.asInstanceOf[Class[T]])
  }

  def toJson(value: Any): String  = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(value)
}
