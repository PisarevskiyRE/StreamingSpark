package chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ReviewAnalyzerStream2 extends App {

  val spark = SparkSession.builder()
    .appName("ReviewAnalyzerStream")
    .master("local[*]")
    .getOrCreate()

  spark.conf.set("spark.sql.session.timeZone", "UTC")

  val schema = StructType(Array(
    StructField("title", StringType),
    StructField("review", StringType),
    StructField("date", TimestampType),
    StructField("rating", IntegerType),
  ))

  val reviewsStreamDf = spark
    .readStream
    .option("header", "true")
    .schema(schema)
    .csv("src/main/resources/reviews")


  val ratingByDayStreamDf = reviewsStreamDf
    .groupBy(date_format(
      col("date"), "MM-dd-yyyy").as("date"))
    .agg(
      avg("rating").alias("avg_rating"))

  ratingByDayStreamDf.writeStream
    .format("console")
    .outputMode("complete")
    .start()
    .awaitTermination()

}
