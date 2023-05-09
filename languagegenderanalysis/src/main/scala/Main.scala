package lga

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, substring}

import scala.io.Source

object Main {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("LanguageGenderAnalysis")
      .getOrCreate()

    //French IPA: https://unalengua.com/ipa-translate?hl=en&ttsLocale=fr-CA&voiceId=Chantal&sl=fr&text=
    //Hindi IPA: https://www.fontconverter.in/index.php?q=Devanagari-to-IPA

    val wordlistFrench = spark.read.option("encoding","utf8").option("header","true").csv("frenchwordlist - 50.csv").
      filter(col("gender") =!= " None")

    val wordlistHindi = spark.read.option("encoding", "utf8").option("header", "true").
      csv("hindiwordlist - 50.csv").
      filter(col("gender")=!=" None")

    wordlistFrench.select("translation").show(50)
    spark.stop()
  }
}