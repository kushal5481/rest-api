package com.experian.ind.nextgen.filevalidation

import org.apache.spark.sql.SparkSession
import com.experian.ind.nextgen.model.FileConfig
import java.io.{File, PrintWriter}
import scala.util.Random

object Ctudf20FixedFileProcessor extends FileProcessorTrait {
    override def process(config: FileConfig, spark: SparkSession): Unit = {

        val folder = new File(config.inputFilePath)

        if(folder.exists && folder.isDirectory) {
            val ctudf20Files = folder.listFiles.filter(file => file.isFile && file.getName.toUpperCase.contains("CTUDF20")).map(_.getAbsolutePath)

            if(ctudf20Files.isEmpty){
                println(s"No files containing 'CTUDF20' found in ${config.inputFilePath}")
                return
            }else{
                println(s"Reading files containing 'CTUDF20'")
                ctudf20Files.foreach(f => println(s" - ${f}"))
            }   
        }else{
            println(s"\n Invalid input Path: ${config.inputFilePath}")
        }

    }
}