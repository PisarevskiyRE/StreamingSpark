package chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}

trait Context {
  val appName: String
  lazy val spark = createSession(appName)

  private def createSession(appName: String) = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
  }
}

object Task2 extends App with Context {

  override val appName: String = "Task2"

  val categorySchema = StructType(Array(
    StructField("category", StringType),
    StructField("category_name", StringType)
  ))

  val aggregatorSchema = StructType(Seq(
    StructField("ID", IntegerType),
    StructField("TITLE", StringType),
    StructField("URL", StringType),
    StructField("PUBLISHER", StringType),
    StructField("CATEGORY", StringType),
    StructField("STORY", StringType),
    StructField("TIMESTAMP", LongType)
  ))


  val categoryStreamDf = spark.readStream
    .option("header", true)
    .schema(categorySchema)
    .csv("src/main/resources/news_category")

  val aggregatorStreamDf = spark.readStream
    .option("header", true)
    .schema(aggregatorSchema)
    .csv("src/main/resources/news_aggregator")


  val joinCondition = categoryStreamDf.col("category") === aggregatorStreamDf.col("CATEGORY")

  val joinedStreamDf = aggregatorStreamDf
    .join(categoryStreamDf, joinCondition)
    .select(
      from_unixtime(col("TIMESTAMP") / 1000).cast(TimestampType).as("Date"),
      col("PUBLISHER").as("Publisher"),
      col("category_name").as("Category")
    )

  val resultStreamDf = joinedStreamDf
    .withWatermark("Date", "1 day")
    .groupBy(
      window(col("Date"), "1 day"),
      col("Publisher"),
      col("Category")
    )
    .agg(count("*").as("Cnt"))

  resultStreamDf.writeStream
    .outputMode(OutputMode.Append)
    .format("console")
    .option("truncate", "false")
    .start()
    .awaitTermination()
}





