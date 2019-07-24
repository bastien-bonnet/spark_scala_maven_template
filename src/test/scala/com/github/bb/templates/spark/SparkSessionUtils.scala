package com.github.bb.templates.spark

import org.apache.spark.sql.SparkSession

trait SparkSessionUtils {

  implicit val spark: SparkSession = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .appName("Automated tests")
      .getOrCreate()

    session.sparkContext.setLogLevel("ERROR")

    session
  }

}
