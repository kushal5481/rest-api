package com.experian.ind.prime.ingest.core.Util.parser.commercial

import org.apache.spark.sql.DataFrame

object DataFrameUtils {
  private lazy val logger = DualLogger(getClass)

  /**
   * Preview a list of DataFrames with configurable limit and raw record length
   * @param dfs List of DataFrames to preview
   * @param limit Number of rows to preview
   * @param recordLength Max length of RawRecord to display
   */
  def previewDataFrames(dfs: List[DataFrame], limit: Int = 20, recordLength: Int = 50): Unit = {
    dfs.zipWithIndex.foreach { case (df, idx) =>
      val sb = new StringBuilder
      sb.append("\n" + "=" * 120 + "\n")
      sb.append(s"Result DataFrame ${idx + 1} Preview (First $limit rows):\n")
      sb.append("=" * 120 + "\n")
      val headers = df.columns
      // Print header row, truncate header names to 50 chars
      sb.append(headers.map(h => f"${h.take(50)}%-50s").mkString("| ", " | ", " |\n"))
      sb.append("-" * (headers.length * 53) + "\n")
      df.limit(limit).collect().foreach { row =>
        val rowStr = headers.map { h =>
          val value = Option(row.getAs[Any](h)).map(_.toString).getOrElse("")
          if (value.length > recordLength) value.take(recordLength) + "..." else value
        }.map(v => f"${v}%-50s").mkString("| ", " | ", " |\n")
        sb.append(rowStr)
      }
      sb.append("=" * 120)
      logger.info(sb.toString)
    }
  }
}
