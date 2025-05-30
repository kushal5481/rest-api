package com.experian.ind.nextgen.filevalidation

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import java.io.InputStream
import com.fasterxml.jackson.databind.{ObjectMapper,JsonNode}
import com.fasterxml.jackson.module.scala.DefaultScalaModule 
import com.experian.ind.nextgen.model.FileConfig
import com.experian.ind.nextgen.filevalidation.{Ctudf20FixedFileProcessor,Btudf30PipeFileProcessor}
import java.nio.file.Files
import java.nio.file.Paths

object FileValidator {

 def main(args: Array[String]): Unit = {
        val config = ConfigFactory.load()
        val sparkConfig = config.getConfig("spark")
        val appName = "FileValidator"
        val master = sparkConfig.getString("master")
        val tempDir = sparkConfig.getString("tempDir")
        val sparkhost = sparkConfig.getString("host")
        println("\n Welcome to FileValidator")
        val spark = SparkSession.builder().appName(appName).master(master).config("spark.local.dir",tempDir).config("spark.driver.host",sparkhost).getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        spark.sparkContext.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive","true")

        
        val fileConfigPath = scala.io.Source.fromResource("file-config.json")
        val fileConfigJson = try
            fileConfigPath.mkString finally
            fileConfigPath.close()

        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)

        val configs = mapper.readValue(fileConfigJson,classOf[Array[FileConfig]]).toList
        println("\n Welcome to FileValidator @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
        configs.filter(_.isActive).foreach{
           config=>
            val filePath = config.inputFilePath
            println(s"\nProcessing file name like : ${config.filename} | format: ${config.format} | path: ${config.inputFilePath}") 

            config.filename match {
                case "CTUDF20" =>
                    Some(Ctudf20FixedFileProcessor).foreach(_.process(config,spark))
                case "BTUDF30" =>
                    Some(Btudf30PipeFileProcessor).foreach(_.process(config,spark))
                case _ => None
            }
        }

        spark.stop
 }
}