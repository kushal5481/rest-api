package com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial

case class ParserConfig(info: ParserInfo, processing: Option[ParserProcessing] = None, logging: ParserLogging, statistics: ParserStatistics, paths: ParserPaths, fileDetails: ParserFileDetails, dataFrames: ParserDataFrames)

case class ParserPaths(logPath: String, outputPath: String, inputPath: Option[String] = None)

  case class ParserInfo(appName: String)

  case class ParserProcessing(
    dbLoadIdStart: Option[Long] = None,
    dbLoadIdEnd: Option[Long] = None,
    batchIdStart: Option[Int] = None,
    batchIdEnd: Option[Int] = None,
    parallelism: Option[Int] = None,
    partitions: Option[Int] = None,
    recordIdStart: Option[Int] = None,
    batchSize: Option[Int] = None,
    enableMultiThreading: Option[Boolean] = None,
    threadPoolSize: Option[Int] = None
  )
  
  case class ParserLogging(logLevel: String, logToFile: Boolean, logToConsole: Boolean)

  case class ParserStatistics(enableStatsLogging: Boolean, statFileName: Option[String] = None)

  case class ParserFileDetails(bureauType: String, fileVersion: String, fileFormat: String, fileName: String)

  case class ParserDataFrames(dataframe1: String, dataframe2: String, dataframe3: String, dataframe4: String)
  
