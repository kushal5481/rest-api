package com.experian.ind.prime.ingest.core.Util.parser.commercial

import java.io.{BufferedWriter, FileWriter}
import java.text.SimpleDateFormat
import java.util.Date

/**
 * Simple file logger that writes directly to a file
 * This bypasses SLF4J/Logback/Log4j to ensure logs are written
 */
object FileLogger {
  
  private var logFilePath: Option[String] = None
  private var writer: Option[BufferedWriter] = None
  private val dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  
  /**
   * Initialize the file logger with a specific path
   */
  def initialize(path: String): Unit = synchronized {
    try {
      close() // Close any existing writer
      logFilePath = Some(path)
      writer = Some(new BufferedWriter(new FileWriter(path, true)))
      writeLog("INFO", "FileLogger", "File logger initialized")
    } catch {
      case e: Exception =>
        println(s"Failed to initialize FileLogger: ${e.getMessage}")
        e.printStackTrace()
    }
  }
  
  /**
   * Write a log entry
   */
  private def writeLog(level: String, loggerName: String, message: String): Unit = synchronized {
      writer.foreach { w =>
        try {
          val timestamp = dateFormatter.format(new Date())
          val threadName = Thread.currentThread().getName
          w.write(s"$timestamp [$threadName] $level $loggerName - $message\n")
          w.flush()
        } catch {
          case e: Exception =>
            println(s"Failed to write log: ${e.getMessage}")
        }
      }
  }
  
  /**
   * Log DEBUG level message
   */
  def debug(loggerName: String, message: String): Unit = {
    writeLog("DEBUG", loggerName, message)
  }
  
  /**
   * Log INFO level message
   */
  def info(loggerName: String, message: String): Unit = {
    writeLog("INFO", loggerName, message)
  }
  
  /**
   * Log WARN level message
   */
  def warn(loggerName: String, message: String): Unit = {
    writeLog("WARN", loggerName, message)
  }
  
  /**
   * Log ERROR level message
   */
  def error(loggerName: String, message: String): Unit = {
    writeLog("ERROR", loggerName, message)
  }
  
  /**
   * Log ERROR level message with exception
   */
  def error(loggerName: String, message: String, throwable: Throwable): Unit = {
    writeLog("ERROR", loggerName, s"$message - ${throwable.getMessage}")
    writeLog("ERROR", loggerName, throwable.getStackTrace.mkString("\n  at "))
  }
  
  /**
   * Close the file logger
   */
  def close(): Unit = synchronized {
    writer.foreach { w =>
      try {
        w.flush()
        w.close()
      } catch {
        case e: Exception =>
          println(s"Error closing FileLogger: ${e.getMessage}")
      }
    }
    writer = None
  }
}

/**
 * Logger that writes only to file using FileLogger
 */
class DualLogger(loggerName: String) {
  def debug(message: String): Unit = {
    FileLogger.debug(loggerName, message)
  }
  def info(message: String): Unit = {
    FileLogger.info(loggerName, message)
  }
  def warn(message: String): Unit = {
    FileLogger.warn(loggerName, message)
  }
  def error(message: String): Unit = {
    FileLogger.error(loggerName, message)
  }
  def error(message: String, throwable: Throwable): Unit = {
    FileLogger.error(loggerName, message, throwable)
  }
}

object DualLogger {
  def apply(clazz: Class[_]): DualLogger = new DualLogger(clazz.getName)
  def apply(name: String): DualLogger = new DualLogger(name)
}
