package chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ReviewAnalyzerPackage extends App {

  val spark = SparkSession.builder()
    .appName("ReviewAnalyzerPackage")
    .master("local[*]")
    .getOrCreate()

  spark.conf.set("spark.sql.session.timeZone", "UTC")

  val reviewsDf = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .csv("src/main/resources/reviews/reviews.csv")

  val lowRatingDf = reviewsDf
    .filter(col("rating") < 8)
    .orderBy(desc("rating"))

  lowRatingDf.show(20)
}
