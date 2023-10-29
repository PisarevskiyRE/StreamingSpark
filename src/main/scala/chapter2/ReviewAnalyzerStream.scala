package chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ReviewAnalyzerStream extends App {

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


  val lowRatingStreamDf = reviewsStreamDf
    .filter(col("rating") < 8)

  lowRatingStreamDf.writeStream
    .format("console")
    .outputMode("append")
    .start()
    .awaitTermination()

}
