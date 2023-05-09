package lga

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, length, substring}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.OneHotEncoder

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

    val wordlistFrench = spark.read.option("encoding","utf8").option("header","true")
      .csv("frenchwordlist - 50.csv").
      filter(col("gender") =!= " None")
      .withColumn("phoneticLast", substring(col("ipa"), -1, 1))
      .withColumn("phoneticLast2", substring(col("ipa"), -2, 2))
      .withColumn("phoneticFirst", substring(col("ipa"), 0, 1))
      .withColumn("phoneticFirst2", substring(col("ipa"), 0, 2))
      .withColumn("translationLast", substring(col("translation"), -1, 1))
      .withColumn("translationLast2", substring(col("translation"), -2, 2))
      .withColumn("translationFirst", substring(col("translation"), 0, 1))
      .withColumn("translationFirst2", substring(col("translation"), 0, 2))
      .withColumn("phoneticLength", length(col("ipa")))
      .withColumn("translationLength", length(col("translation")))

    val wordlistHindi = spark.read.option("encoding", "utf8").option("header", "true").
      csv("hindiwordlist - 50.csv").
      filter(col("gender")=!=" None")
      .withColumn("phoneticLast", substring(col("ipa"), -1, 1))
      .withColumn("phoneticLast2", substring(col("ipa"), -2, 2))
      .withColumn("phoneticFirst", substring(col("ipa"), 0, 1))
      .withColumn("phoneticFirst2", substring(col("ipa"), 0, 2))
      .withColumn("translationLast", substring(col("translation"), -1, 1))
      .withColumn("translationLast2", substring(col("translation"), -2, 2))
      .withColumn("translationFirst", substring(col("translation"), 0, 1))
      .withColumn("translationFirst2", substring(col("translation"), 0, 2))
      .withColumn("phoneticLength", length(col("ipa")))
      .withColumn("translationLength", length(col("translation")))


    val wordlistHindiRenamed = wordlistHindi.toDF(wordlistHindi.columns.map("Hindi_"+_): _*)
    val wordlistFrenchRenamed = wordlistFrench.toDF(wordlistFrench.columns.map("French_" + _): _*)
    val joinedList = wordlistHindiRenamed.join(right=wordlistFrenchRenamed,
      joinExprs=col("Hindi_word")===col("French_word"), joinType="inner").drop("Hindi_word", "Hindi_ipa", "French_ipa")

    write(wordlistHindiRenamed, "wordlistHindi")
    write(wordlistFrenchRenamed, "wordlistFrench")
    write(joinedList, "joinedList")
//    joinedList.show(50)
//    wordlistHindi.write.options(Map("Encoding"->"utf8", "header"->"true")).csv("wordlistHindi")
//    wordlistFrench.write.options(Map("Encoding" -> "utf8", "header" -> "true")).csv("D:\\wordlistFrench")
//    joinedList.write.options(Map("Encoding" -> "utf8", "header" -> "true")).csv("D:\\joinedList")
    spark.stop()
  }

  import java.io._
  def write(df: DataFrame, name: String): Unit = {
    val pw = new PrintWriter(new File(name+".csv"))
    pw.write(df.collect.map(_.toSeq).mkString("\n"))
    pw.close

  }
}