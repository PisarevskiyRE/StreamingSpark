package chapter2

import org.apache.spark.sql.SparkSession
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


  iotStreamDf.writeStream
    .format("console")
    .outputMode("append")
    .start()
    .awaitTermination()

}
