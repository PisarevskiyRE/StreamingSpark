package chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ReviewAnalyzerPackage2 extends App {

  val spark = SparkSession.builder()
    .appName("ReviewAnalyzerPackage")
    .master("local[*]")
    .getOrCreate()

  spark.conf.set("spark.sql.session.timeZone", "UTC")

  val schema = StructType(Array(
    StructField("title", StringType),
    StructField("review", StringType),
    StructField("date", TimestampType),
    StructField("rating", IntegerType),
  ))


  val reviewsDf = spark.read
    .option("header", true)
    .schema(schema)
    .csv("src/main/resources/reviews/reviews.csv")

  val ratingByDayDf = reviewsDf
    .groupBy(date_format(col("date"), "MM-dd-yyyy").as("date"))
    .agg(
      avg("rating").alias("avg_rating")
    )
    .orderBy(desc("avg_rating"))

  ratingByDayDf.show

}
