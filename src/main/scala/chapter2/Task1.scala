package chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
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


  val filterStreamDF = iotStreamDf
    .withColumn("temp_celsium",
      (col("temp") - 32) * 5/9)
    .withColumn("datetime", to_timestamp(col("ts").cast(TimestampType)))
    .drop("ts")
    .drop("co")
    .drop("humidity")
    .drop("light")
    .drop("lpg")
    .drop("motion")
    .drop("smoke")
    .drop("temp")

  filterStreamDF.writeStream
    .outputMode("append")
    .format("console")
    .start()
    .awaitTermination()


//  val dataStreamDf = iotStreamDf
//    .withColumn("temp_celsius",
//      (col("temp") - 32) * 5 / 9
//    )
//    .withColumn("datetime", to_timestamp(col("ts").cast(TimestampType)))
//    .withWatermark("datetime", "2 minutes")
//    .groupBy(window(col("datetime"), "2 minutes"), col("device"))
//    .agg(avg(col("temp_celsius")).as("avg_temp_celsius"))
//
//
//  val result = dataStreamDf.writeStream
//    .outputMode("append")
//    .format("console")
//    .trigger(Trigger.ProcessingTime("1 minute"))
//    .start()


  //result.awaitTermination()

}
