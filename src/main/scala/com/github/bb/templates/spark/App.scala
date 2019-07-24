package com.github.bb.templates.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object App {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark Scala Project Template")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val data = List(args(0).toInt, args(1).toInt).toDF("id")

    val result = data.withColumn("id", sqrt("id"))

    val outputPath = args(2)
    result.write.parquet(outputPath)
  }

}
