package com.experian.ind.prime.ingest.core.rules_management

import com.experian.ind.prime.ingest.core.Util.JsonUtil
import com.experian.ind.prime.ingest.core.shared_models.schema_models.{FieldDetailModel, FileMainConfigModel, RecordDetailModel, SegmentDetailModel}

import java.io.File
import scala.io.Source

object SchemaLoader {
    def loadSchema(schemaPath: String): List[FileMainConfigModel] = {
      val json = Source.fromFile(schemaPath).mkString
      JsonUtil.fromJson[List[FileMainConfigModel]](json)
    }
  }