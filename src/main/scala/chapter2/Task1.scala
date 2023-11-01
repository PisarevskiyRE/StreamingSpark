package chapter2

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Task1 extends App{

  val spark = SparkSession.builder()
    .appName("Task1")
    .master("local")
    .getOrCreate()

  val schema = StructType(Seq(
    StructField("ts", DoubleType),
    StructField("device", StringType),
    StructField("co", DoubleType),
    StructField("humidity", DoubleType),
    StructField("light", BooleanType),
    StructField("lpg", DoubleType),
    StructField("motion", BooleanType),
    StructField("smoke", DoubleType),
    StructField("temp", DoubleType)
  ))


  val iotStreamDf = spark.readStream
    .option("header",true)
    .schema(schema)
    .csv("src/main/resources/task1")


  def toCelsius(temp: Column): Column = {
    (temp - 32) * 5/9
  }


  val iotCleanStreamDf = iotStreamDf
    .withColumn("temp_celsius",
      toCelsius(col("temp"))
    )
    .withColumn("datetime",
      to_timestamp(col("ts").cast(TimestampType))
    )
    .drop("ts")
    .drop("co")
    .drop("humidity")
    .drop("light")
    .drop("lpg")
    .drop("motion")
    .drop("smoke")
    .drop("temp")


  val iotWindowedStreamDf = iotCleanStreamDf
    .withWatermark("datetime", "2 minutes")
    .groupBy(
      window(col("datetime"),
        "2 minutes",
        "1 minutes"),
      col("device")
    )
    .agg(
      avg(col("temp_celsius"))
    )


  iotWindowedStreamDf.writeStream
    .outputMode("append")
    .format("console")
    .start()
    .awaitTermination()
}
