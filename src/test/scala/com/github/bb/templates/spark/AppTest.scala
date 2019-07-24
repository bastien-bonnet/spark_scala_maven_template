package com.github.bb.templates.spark

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalatest._

import scala.reflect.ClassTag

class AppTest extends FunSuite with Matchers with BeforeAndAfterAll with SparkSessionUtils {

  private val OUTPUT_FOLDER = "src/test/resources/out.csv"

  override def beforeAll(): Unit = {
    super.beforeAll()
    FileUtils.deleteDirectory(new File(OUTPUT_FOLDER))
  }

  override def afterAll(): Unit = {
    FileUtils.deleteDirectory(new File(OUTPUT_FOLDER))
  }

  test("Test sample") {
    // GIVEN


    // WHEN
    App.main(Array("4", "9", OUTPUT_FOLDER))

    // THEN
    val outputData = spark.read.parquet(OUTPUT_FOLDER)
    val longs = getFirstColumn[Double](outputData)
    longs should contain theSameElementsAs Set(2.0, 3.0)
  }

  private def getFirstColumn[T: ClassTag](outputData: DataFrame): Set[T] = {
    outputData.collect().map(_.getAs[T](0)).toSet[T]
  }
}
