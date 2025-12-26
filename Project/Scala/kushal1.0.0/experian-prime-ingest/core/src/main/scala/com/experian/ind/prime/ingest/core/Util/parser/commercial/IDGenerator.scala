package com.experian.ind.prime.ingest.core.Util.parser.commercial

import scala.util.Random

/**
 * ID Generator for fileLoadId, BatchID, and RecordID
 */
object IDGenerator {
  
  private lazy val logger = DualLogger(getClass)
  // Read ID generation ranges from parser-config.json (cached by ConfigLoader)
  // Use JSON-backed getters to avoid Integer vs Long casting issues
  private val dbLoadIdStart = ConfigLoader.getParserLongOrElse(10000L, "processing", "dbLoadIdStart")
  private val dbLoadIdEnd   = ConfigLoader.getParserLongOrElse(99999L, "processing", "dbLoadIdEnd")
  private val batchIdStart  = ConfigLoader.getParserIntOrElse(10000, "processing", "batchIdStart")
  private val batchIdEnd    = ConfigLoader.getParserIntOrElse(99999, "processing", "batchIdEnd")

      
  /**
   * Generate unique fileLoadId for the input file
   * Format: YYYYMMDDHHMMSS + 5-digit random number (to fit in Long)
   * Example: 20251126134325 + 12345 = 2025112613432512345
   */
  def generateDBLoadId(): Long = {
    val timestamp = new java.text.SimpleDateFormat("yyyyMMddHHmmss").format(new java.util.Date())
    val random = new Random()
    // Use configured range for random part
    val range = (dbLoadIdEnd - dbLoadIdStart + 1).toInt
    val randomPart = dbLoadIdStart + random.nextInt(range)
    val fileLoadId = s"$timestamp$randomPart".toLong
    //logger.info(s"Generated fileLoadId: $fileLoadId (timestamp: $timestamp, random: $randomPart)")
    fileLoadId
  }
  
  /**
   * Generate unique BatchID with wraparound support
   * When the range is exhausted, it wraps around to the start
   */
  def generateBatchId(): Int = {
    val range = batchIdEnd - batchIdStart + 1
    val batchId = batchIdStart + Random.nextInt(range)
    //logger.info(s"Generated BatchID: $batchId")
    batchId
  }
  
  /**
   * Generate RecordID with counter and unique suffix
   * Format: counter-xxx9999xxx (3 letters + 4 digits + 3 letters + 3 digits)
   * Supports 500M+ records with high uniqueness
   */
  def generateRecordId(counter: Int): String = {
    val random = new Random()
    
    // Generate 3 random lowercase letters
    val threeLetters1 = (1 to 3).map(_ => ('a' + random.nextInt(26)).toChar).mkString
    
    // Generate 4 random digits
    val fourDigits = (1 to 4).map(_ => random.nextInt(10)).mkString
    
    // Generate 3 random lowercase letters
    val threeLetters2 = (1 to 3).map(_ => ('a' + random.nextInt(26)).toChar).mkString
    
    // Generate 3 random digits
    val threeDigits = (1 to 3).map(_ => random.nextInt(10)).mkString
    
    s"$counter-$threeLetters1$fourDigits$threeLetters2$threeDigits"
  }
}
