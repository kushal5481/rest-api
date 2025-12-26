package com.experian.ind.prime.ingest.core.Util.parser.commercial
import com.experian.ind.prime.ingest.core.shared_models.parser_models.commercial.{FileConfig, FileFormatConfig, MetadataConfig, ParserConfig}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.io.FileInputStream
import java.util.Properties
import scala.util.Using

/**
 * Configuration loader for metadata and file format configurations
 */
object ConfigLoader {
  
  private lazy val logger = DualLogger(getClass)
  private val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)
  
  // Configure ObjectMapper to handle unknown properties dynamically
  // This allows JSON to have new fields without failing deserialization
  import com.fasterxml.jackson.databind.DeserializationFeature
  objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  // Caches to avoid repeated IO on config files
  @volatile private var cachedMetadata: Option[MetadataConfig] = None
  // Cache both typed model and Json view of parser-config
  @volatile private var cachedParserConfigModel: Option[ParserConfig] = None
  @volatile private var cachedParserConfigJson: Option[JsonNode] = None
  @volatile private var cachedFileFormatConfigModel: Option[FileFormatConfig] = None
  @volatile private var cachedFileFormatConfigJson: Option[JsonNode] = None
  @volatile private var cachedFileConfigModel: Option[FileConfig] = None
  @volatile private var cachedFileConfigJson: Option[JsonNode] = None

  /**
   * Initialize and cache all required configs at application startup.
   * This method is idempotent and safe to call multiple times.
   */
  def init(): Unit = synchronized {
    try{
      println("Initializing ConfigLoader and caching configurations...")
      if (cachedMetadata.isEmpty) {
      val metadata = loadMetadataConfig()
      cachedMetadata = Some(metadata)
      println("Metadata configuration cached")
      // Load dependent configs after metadata
      val parserCfg = loadParserConfig()
      cachedParserConfigModel = Some(parserCfg)
      cachedParserConfigJson = Some(objectMapper.valueToTree(parserCfg))
      val fileFormatConfig = loadFileFormatConfig(metadata)
      cachedFileFormatConfigModel = Some(fileFormatConfig)
      cachedFileFormatConfigJson = Some(objectMapper.valueToTree(fileFormatConfig))
      val fileConfig = loadFileConfig(metadata)
      cachedFileConfigModel = Some(fileConfig)
      cachedFileConfigJson = Some(objectMapper.valueToTree(fileConfig))
      println("Parser and file format configurations cached")
    } else {
      println("ConfigLoader already initialized; using cached configurations")
    }
    } catch {
      case ex: Exception =>
        println(s"Error during ConfigLoader initialization: ${ex.getMessage}", ex)
        throw ex
    }    
  }
  
  /**
   * Load metadata configuration from metaData.properties
   */
  private def loadMetadataConfig(): MetadataConfig = {    
    //logger.info(s"Loading metadata from metaData.properties")
    var bureauType: String  = ""
    var fileVersion: String  = ""
    var fileFormat: String  = ""
    var fileName: String  = ""
    var inputPath: String  = ""
    var propPath: String  = ""
    val properties = new Properties()

    try{ 
      propPath = System.getProperty("META_DATA_PATH")
      val file = new java.io.File(propPath)
      if (!file.exists()) {
        val msg = s"META_DATA_PATH is invalid : ${file.getAbsolutePath}. "
        throw new java.io.FileNotFoundException(msg)
      }
      val inputStream = new FileInputStream(propPath)   
      if (inputStream == null) throw new IllegalArgumentException(s"metaData file not found in : $propPath")  
      properties.load(inputStream)
      bureauType = properties.getProperty("parser.bureau_type")
      fileVersion = properties.getProperty("parser.input_file_version")
      fileFormat = properties.getProperty("parser.input_file_format")
      fileName = properties.getProperty("parser.input_file_name")
      inputPath = properties.getProperty("parser.input_path")
      //logger.info(s"Metadata loaded: bureauType=$bureauType, fileVersion=$fileVersion, fileFormat=$fileFormat, fileName=$fileName, inputPath=$inputPath")
    MetadataConfig(industry = bureauType, fileVersion = fileVersion, fileFormat = fileFormat, fileName = fileName, inputPath = inputPath)
    } catch {
      case ex: Exception => println(s"Error while loading metadata : ${ex.getMessage}",ex)
        throw ex
    }    
  }  

  private def loadParserConfig(): ParserConfig = {
    //logger.info("Loading parser configuration from parser-config.json")
    var jsonPath: String  = ""
    try{
      jsonPath = System.getProperty("PARSER_CONFIG_PATH")
      val file = new java.io.File(jsonPath)
      if (!file.exists()) {
        val msg = s"PARSER_CONFIG_PATH is invalid : ${file.getAbsolutePath}. "
        throw new java.io.FileNotFoundException(msg)
      }
      val inputStream = new FileInputStream(jsonPath)
      if (inputStream == null) throw new IllegalArgumentException(s"parser-config file not found in : $jsonPath")
      val cfg = Using.resource(inputStream) { in =>
        objectMapper.readValue(in, classOf[ParserConfig])
      }
      // Basic validation of required fields for typed access
      val missing = scala.collection.mutable.ArrayBuffer[String]()
      if (cfg.paths == null || cfg.paths.logPath == null || cfg.paths.logPath.trim.isEmpty) missing += "paths.logPath"
      if (cfg.paths == null || cfg.paths.outputPath == null || cfg.paths.outputPath.trim.isEmpty) missing += "paths.outputPath"
      if (cfg.info == null || cfg.info.appName == null || cfg.info.appName.trim.isEmpty) missing += "info.appName"
      if (cfg.logging == null || cfg.logging.logLevel == null || cfg.logging.logLevel.trim.isEmpty) missing += "logging.logLevel"
      if (cfg.logging == null) missing += "logging"
      if (missing.nonEmpty) {
        throw new IllegalArgumentException("parser-config missing required fields: " + missing.mkString(", "))
      }
      cfg
    } catch {
      case ex: Exception =>
        println(s"Error while loading parser-config : ${ex.getMessage}", ex)
        throw ex
    }
  }
  
  /**
   * Load file format configuration JSON based on metadata
   */
  private def loadFileFormatConfig(metadata: MetadataConfig): FileFormatConfig = {
    try{
    val fileConfig = loadFileConfig(metadata)
    val formatFilePath = fileConfig.templateLocation + fileConfig.jsonTemplate
    //logger.info(s"Loading file format from json : $formatFilePath")

    val file = new java.io.File(formatFilePath)
      if (!file.exists()) {
        val msg = s"Format File Path is invalid : ${file.getAbsolutePath}. "
        throw new java.io.FileNotFoundException(msg)
      }
    
    val inputStream = new FileInputStream(formatFilePath)
    if (inputStream == null) throw new IllegalArgumentException(s"UCRF Format file not found in : $formatFilePath")
    Using.resource(inputStream) { in =>
      objectMapper.readValue(in, classOf[FileFormatConfig])
    }
    } catch {
      case ex: Exception =>
        println(s"Error while loading file-format config : ${ex.getMessage}", ex)
        throw ex
    }
  }  
  
  /**
   * Get file configuration from file-config.json based on metadata
   */
  private def loadFileConfig(metadata: MetadataConfig): FileConfig = {
    //logger.info(s"Loading file configuration from file-config.json")    
    var jsonPath: String  = ""
try{
    jsonPath = System.getProperty("FILE_CONFIG_PATH")
    val file = new java.io.File(jsonPath)
      if (!file.exists()) {
        val msg = s"FILE_CONFIG_PATH is invalid : ${file.getAbsolutePath}. "
        throw new java.io.FileNotFoundException(msg)
      }
    val inputStream = new FileInputStream(jsonPath)
    if (inputStream == null) throw new IllegalArgumentException(s"file-config not found in : $jsonPath")
    val fileConfigs = Using.resource(inputStream) { in =>
      objectMapper.readValue(in, classOf[Array[FileConfig]])
    }
    
    // Find matching configuration
    fileConfigs.find { config =>
      config.version == metadata.fileVersion &&
      config.format == metadata.fileFormat &&
      config.industry == metadata.industry
    }.getOrElse {
      throw new IllegalArgumentException(
        s"No file configuration found for: version=${metadata.fileVersion}, " +
        s"format=${metadata.fileFormat}, industry=${metadata.industry}"
      )
    }
} catch {
      case ex: Exception =>
        println(s"Error while loading file-config : ${ex.getMessage}", ex)
        throw ex
    }
  }

  /**
   * Get input file path from metadata
   */
  def getInputFilePath(metadata: MetadataConfig): String = {
    val baseDir = s"${metadata.inputPath}"
    //logger.info(s"Input file base directory: $baseDir")
    val fileName = s"${metadata.fileName}"
    //logger.info(s"Input file name: $fileName")
    s"$baseDir/$fileName"
  }
  
  /**
   * Get output directory path for processed files
   */
  def getOutputPath(metadata: MetadataConfig): String = {
    // Read output path from parser-config.json only
    val baseDir = getParserString("paths", "outputPath").getOrElse {
      throw new IllegalArgumentException("parser-config: paths.outputPath is missing")
    }
    //logger.info(s"Output file base directory: $baseDir")
    baseDir
  }

  /** Access cached metadata; triggers init if needed */
  def metadataConfig(): MetadataConfig = {
    if (cachedMetadata.isEmpty) init()
    cachedMetadata.get
  }

  /** Access cached parser-config; triggers init if needed */
  def parserConfigModel(): ParserConfig = {
    if (cachedParserConfigModel.isEmpty) init()
    cachedParserConfigModel.get
  }

  def parserConfigJson(): JsonNode = {
    if (cachedParserConfigJson.isEmpty) init()
    cachedParserConfigJson.get
  }

  /** Access cached file format config; triggers init if needed */
  def fileFormatConfigModel(): FileFormatConfig = {
    if (cachedFileFormatConfigModel.isEmpty) init()
    cachedFileFormatConfigModel.get
  }

  def fileFormatConfigJson(): JsonNode = {
    if (cachedFileFormatConfigJson.isEmpty) init()
    cachedFileFormatConfigJson.get
  }

  /** Access cached file config; triggers init if needed */
  def fileConfigModel(): FileConfig = {
    if (cachedFileConfigModel.isEmpty) init()
    cachedFileConfigModel.get
  }

  def fileConfigJson(): JsonNode = {
    if (cachedFileConfigJson.isEmpty) init()
    cachedFileConfigJson.get
  }

  /**
   * Efficiently fetch a nested string from parser-config via path segments.
   * Example: getParserString("paths", "logPath")
   */
  def getParserString(pathSegments: String*): Option[String] = {
    val root = parserConfigJson()
    var node: JsonNode = root
    for (seg <- pathSegments) {
      if (node == null) return None
      node = node.path(seg)
    }
    if (node != null && !node.isMissingNode && !node.isNull) Option(node.asText()) else None
  }

  /** Fetch with default value if missing */
  def getParserStringOrElse(default: String, pathSegments: String*): String = {
    getParserString(pathSegments: _*).getOrElse(default)
  }

  /** Fetch a Long from parser-config safely (handles Integer vs Long) */
  def getParserLong(pathSegments: String*): Option[Long] = {
    val root = parserConfigJson()
    var node: JsonNode = root
    for (seg <- pathSegments) {
      if (node == null) return None
      node = node.path(seg)
    }
    if (node != null && !node.isMissingNode && !node.isNull) Option(node.asLong()) else None
  }

  def getParserLongOrElse(default: Long, pathSegments: String*): Long = {
    getParserLong(pathSegments: _*).getOrElse(default)
  }

  /** Fetch an Int from parser-config safely */
  def getParserInt(pathSegments: String*): Option[Int] = {
    val root = parserConfigJson()
    var node: JsonNode = root
    for (seg <- pathSegments) {
      if (node == null) return None
      node = node.path(seg)
    }
    if (node != null && !node.isMissingNode && !node.isNull) Option(node.asInt()) else None
  }

  def getParserIntOrElse(default: Int, pathSegments: String*): Int = {
    getParserInt(pathSegments: _*).getOrElse(default)
  }

  def getFileFormatString(pathSegments: String*): Option[String] = {
    val root = fileFormatConfigJson()
    var node: JsonNode = root
    for (seg <- pathSegments) {
      if (node == null) return None
      node = node.path(seg)
    }
    if (node != null && !node.isMissingNode && !node.isNull) Option(node.asText()) else None
  }

  /** Fetch with default value if missing */
  def getFileFormatStringOrElse(default: String, pathSegments: String*): String = {
    getFileFormatString(pathSegments: _*).getOrElse(default)
  }

  /** Fetch with default value if missing */
  def getFileConfigStringOrElse(default: String, pathSegments: String*): String = {
    getFileConfigString(pathSegments: _*).getOrElse(default)
  }
  
  def getFileConfigString(pathSegments: String*): Option[String] = {
    val root = fileConfigJson()
    var node: JsonNode = root
    for (seg <- pathSegments) {
      if (node == null) return None
      node = node.path(seg)
    }
    if (node != null && !node.isMissingNode && !node.isNull) Option(node.asText()) else None
  }
  
}
