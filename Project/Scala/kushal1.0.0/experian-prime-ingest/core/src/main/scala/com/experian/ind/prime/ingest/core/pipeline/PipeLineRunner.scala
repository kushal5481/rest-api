package com.experian.ind.prime.ingest.core.pipeline

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.core.FileAppender
import com.experian.ind.prime.ingest.core.Util.parser.commercial.{ConfigLoader, FileLogger, IDGenerator, Stopwatch}
import com.experian.ind.prime.ingest.core.orchestrator.Orchestrator
import com.experian.ind.prime.ingest.core.shared_models.PipelineContext
import com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial.{FileConfig, FileFormatConfig, MetadataConfig, ParserConfig}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object PipeLineRunner {

  private var metadata: MetadataConfig = _
  private var parserConfigModel: ParserConfig = _
  private var fileFormatConfigModel: FileFormatConfig = _
  private var fileConfigModel: FileConfig = _
  
  def main(args: Array[String]): Unit = {
    println("===== Pipeline Runner started =====")
    val stopwatch = Stopwatch.createStarted()
    var fileLoadId: Long = 0L
    var spark: SparkSession = null

    // For local testing - set parser config path
    val localConfigPath = "C:/Users/C19269E/Kushal/Workspaces/VSCode/kushal1.0.0/experian-prime-ingest/properties/parser/commercial/config"
    
    try{
    System.setProperty("PARSER_CONFIG_PATH", s"$localConfigPath/parser-config.json")
    System.setProperty("FILE_CONFIG_PATH", s"$localConfigPath/file-config.json")
    System.setProperty("META_DATA_PATH", s"$localConfigPath/metaData.properties")

    // Initialize commercial parser configs (metaData.properties, parser-config.json, file-config.json)
    ConfigLoader.init()

    metadata = ConfigLoader.metadataConfig()
      parserConfigModel = ConfigLoader.parserConfigModel()
      fileFormatConfigModel = ConfigLoader.fileFormatConfigModel()
      fileConfigModel = ConfigLoader.fileConfigModel()
      println(s"Configuration loaded: Metadata(file=${metadata.fileName}, path=${metadata.inputPath}), Parser(app=${parserConfigModel.info.appName}), FileFormat(format=${fileFormatConfigModel.fileMetaData.format})")
    } catch {
      case e: Exception =>
        println(s"Error during ConfigLoader initialization: ${e.getMessage}", e)
        throw e
    }

try{
    // Clean up old Spark temp directories from previous runs
    cleanupOldSparkTempDirs()
} catch {
      case e: Exception =>
        println(s"Error during cleanup of old Spark temp directories: ${e.getMessage}", e)
        // Proceeding despite cleanup failure
    }

    try{
// Setup log file path before any logging and capture fileLoadId
    fileLoadId = setupLogFilePath()    
      
println(s"File Load ID: $fileLoadId")
    try {
        spark = initializeSparkSession()
        println("Spark Session initialized successfully")
      } catch {
        case e: Exception =>
          println(s"Failed to initialize Spark Session: ${e.getMessage}", e)
          throw e
      }
      
    val context = PipelineContext(spark,metadata,parserConfigModel,fileFormatConfigModel, fileConfigModel, fileLoadId)
    Orchestrator.start(context)

    } catch {
      case e: Exception =>
        println(s"Error in PipeLineRunner: ${e.getMessage}", e)
    } finally {
      // Cleanup
      if (spark != null) {
        println("Stopping Spark Session...")
        try {
          // Uncache all cached data before stopping
          spark.catalog.clearCache()
          spark.stop()
          println("Spark Session stopped successfully")
          
          // Give Windows time to release file handles
          Thread.sleep(1000)
        } catch {
          case e: Exception =>
            println(s"Warning during Spark shutdown: ${e.getMessage}")
        }
      }
      
      println(s"Total Execution Time: ${stopwatch.elapsed()}")
    }
  }

  /**
   * Clean up old Spark temp directories from previous runs
   * This addresses the Windows file lock issue where temp files can't be deleted on shutdown
   */
  private def cleanupOldSparkTempDirs(): Unit = {
    try {
      val tempDir = new java.io.File(System.getProperty("java.io.tmpdir"))
      val sparkDirs = tempDir.listFiles(new java.io.FileFilter {
        override def accept(file: java.io.File): Boolean = {
          file.isDirectory && file.getName.startsWith("spark-")
        }
      })
      
      if (sparkDirs != null) {
        var cleaned = 0
        var failed  = 0
        sparkDirs.foreach { dir =>
          try {
            val ageMs = System.currentTimeMillis() - dir.lastModified()
            if (ageMs > 10000) { // > 10s old
              deleteDirectory(dir)
              cleaned += 1
            }
          } catch {
            case _: Exception => failed += 1
          }
        }
        if (cleaned > 0 || failed > 0) println(s"Spark temp cleanup: cleaned=$cleaned, failed=$failed")
      }
    } catch {
      case _: Exception => () // keep quiet on cleanup issues
    }
  }
  
  /**
   * Recursively delete directory
   */
  private def deleteDirectory(dir: java.io.File): Unit = {
    if (dir.exists()) {
      val files = dir.listFiles()
      if (files != null) {
        files.foreach { file =>
          if (file.isDirectory) {
            deleteDirectory(file)
          } else {
            file.delete()
          }
        }
      }
      dir.delete()
    }
  }
  
  /**
   * Setup log file path dynamically based on fileLoadId
   * @return Generated fileLoadId to be used throughout the application
   */
  private def setupLogFilePath(): Long = {
    var fileLoadId: Long = 0L
    try {
      val logPath    = parserConfigModel.paths.logPath
      val appName    = parserConfigModel.info.appName
      val logToFile  = parserConfigModel.logging.logToFile
      val logToConsole = parserConfigModel.logging.logToConsole
      val logLevel   = parserConfigModel.logging.logLevel.toUpperCase

      val level = logLevel match {
        case "TRACE" => ch.qos.logback.classic.Level.TRACE
        case "DEBUG" => ch.qos.logback.classic.Level.DEBUG
        case "WARN"  => ch.qos.logback.classic.Level.WARN
        case "ERROR" => ch.qos.logback.classic.Level.ERROR
        case _        => ch.qos.logback.classic.Level.INFO
      }

      fileLoadId = IDGenerator.generateDBLoadId()

      val logDir = new java.io.File(logPath)
      if (!logDir.exists()) logDir.mkdirs()

      val logFilePath = s"$logPath/${fileLoadId}_${appName}_log.txt"
      System.setProperty("LOG_FILE_PATH", logFilePath)

      val loggerFactory = LoggerFactory.getILoggerFactory
      loggerFactory match {
        case ctx: LoggerContext =>
          val root = ctx.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
          root.detachAndStopAllAppenders()

          if (logToConsole) {
            val console = new ch.qos.logback.core.ConsoleAppender[ch.qos.logback.classic.spi.ILoggingEvent]()
            console.setContext(ctx)
            console.setName("CONSOLE")
            val cEnc = new PatternLayoutEncoder()
            cEnc.setContext(ctx)
            cEnc.setPattern("%d{yyyy-MM-dd HH:mm:ss} %-5level %logger{36} - %msg%n")
            cEnc.start()
            console.setEncoder(cEnc)
            console.start()
            root.addAppender(console)
          }

          if (logToFile) {
            val file = new FileAppender[ch.qos.logback.classic.spi.ILoggingEvent]()
            file.setContext(ctx)
            file.setName("FILE")
            file.setFile(logFilePath)
            file.setAppend(true)
            file.setImmediateFlush(true)
            val fEnc = new PatternLayoutEncoder()
            fEnc.setContext(ctx)
            fEnc.setPattern("%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n")
            fEnc.start()
            file.setEncoder(fEnc)
            file.start()
            root.addAppender(file)
          }

          root.setLevel(level)

        case _ =>
          // Not Logback; skip programmatic configuration
      }

      // Ensure file exists for FileLogger
      if (logToFile) {
        val f = new java.io.File(logFilePath)
        if (!f.exists()) new java.io.FileWriter(logFilePath, true).close()
        FileLogger.initialize(logFilePath)
        FileLogger.info("CommercialFileParserService", s"=== Application Started with fileLoadId: $fileLoadId ===")
      }

      // Single concise summary line of logging setup
      println(s"Logging configured (console=$logToConsole, file=$logToFile, level=${level.levelStr}, path=$logFilePath)")

    } catch {
      case e: Exception =>
        println(s"Failed to setup log file path: ${e.getMessage}")
        if (fileLoadId == 0L) fileLoadId = IDGenerator.generateDBLoadId()
    }
    fileLoadId
  }

  /**
   * Initialize Spark Session with configurations
   */
  private def initializeSparkSession(): SparkSession = {
    val parallelism = parserConfigModel.processing.flatMap(_.parallelism).getOrElse(4).toString
    val partitions = parserConfigModel.processing.flatMap(_.partitions).getOrElse(4).toString
    val javaModuleArgs = "--add-opens java.base/sun.security.action=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED"
    
    val spark = SparkSession.builder()
      .appName("Experian-Prime-Ingest")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
      .config("spark.sql.shuffle.partitions", partitions.toString)
      .config("spark.default.parallelism", parallelism.toString)
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      // Use JavaSerializer to avoid Java 17 module-access issues
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      // JVM module access for Java 9+ compatibility with Spark serialization
      .config("spark.driver.extraJavaOptions", javaModuleArgs)
      .config("spark.executor.extraJavaOptions", javaModuleArgs)
      .config("spark.driver.memory", "4g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()
    // Set log level
    spark.sparkContext.setLogLevel("WARN")
    println(s"Spark initialized (parallelism=$parallelism, partitions=$partitions)")
    println(s"Applied JVM module access arguments for Java 17 compatibility")
    spark
  }
}
