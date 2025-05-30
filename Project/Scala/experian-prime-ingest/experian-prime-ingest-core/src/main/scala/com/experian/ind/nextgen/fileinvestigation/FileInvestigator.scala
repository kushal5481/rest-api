package com.experian.ind.nextgen.fileinvestigation

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import java.nio.file.{Files,Paths}
import java.io.File
import java.text.SimpleDateFormat


object FileInvestigator{
    def main(args: Array[String]): Unit = {
        val config = ConfigFactory.load()
        val sparkConfig = config.getConfig("spark")
        val sparkhost = sparkConfig.getString("host")
        val appName = sparkConfig.getString("appName")
        val master = sparkConfig.getString("master")
        val tempDir = sparkConfig.getString("tempDir")
        val inputPath = sparkConfig.getString("inputPath")

        println(s"\n Welcome to $appName")
        println(s"\n Investigation files : $inputPath")

        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val inputDir = new File(inputPath)

        if(inputDir.exists && inputDir.isDirectory){
            val files = inputDir.listFiles().filter(_.isFile)
            println(s"\n Found ${files.length} files with metadata:\n")
            files.foreach{file=>
                val fileName = file.getName
                val fileSize = file.length()
                val lastModified = sdf.format(file.lastModified())
                println(s" File Name : $fileName")
                println(s" File Size : $fileSize")
                println(s" File Last Modified : $lastModified")
                }
        }
        else{
            println(s"Invalid Directory Path: $inputPath")
            return
        }

        val spark = SparkSession.builder().appName(appName).master(master).config("spark.local.dir",tempDir).config("spark.driver.host",sparkhost).getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        spark.sparkContext.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive","true")

        val inputRDD = spark.sparkContext.wholeTextFiles(s"$inputPath/*.txt")
        val fileLineCounts = inputRDD.map {
            case (fileName , content) =>
                val lines = content.split("\n")
                println(s" File: $fileName")
                println(s" Line Count: ${lines.length}")
                (fileName,lines.length)
        }

        val totalFiles = fileLineCounts.count()
          println(s" Processed ${totalFiles} files.")

        spark.stop()

    }
}