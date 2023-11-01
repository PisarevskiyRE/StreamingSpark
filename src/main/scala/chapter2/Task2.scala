package chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

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
    StructField("TIMESTAMP", TimestampType)
  ))


  val categoryStreamDf = spark.readStream
    .option("header", true)
    .schema(categorySchema)
    .csv("src/main/resources/news_category")

  val aggregatorStreamDf = spark.readStream
    .option("header", true)
    .schema(categorySchema)
    .csv("src/main/resources/news_aggregator")



//  categoryStreamDf.writeStream
//    .outputMode(OutputMode.Append)
//    .format("console")
//    .start()
//    //.awaitTermination()
//
//  aggregatorStreamDf.writeStream
//    .outputMode(OutputMode.Append)
//    .format("console")
//    .start()
//    .awaitTermination()
//


}
