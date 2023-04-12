package lga

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("LanguageGenderAnalysis")
      .getOrCreate()

    println(spark.getClass())
    spark.stop()
  }
}