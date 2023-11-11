package chapter3

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.{avg, col}
import org.apache.spark.sql.streaming.OutputMode

object Task3_a extends App with Context {

  case class Record(timestamp: Timestamp, value: Long)

  override val appName: String = "rate"

  val streamDf = spark
    .readStream
    .format("rate")
    .option("rowsPerSecond", 3)
    .load

  import spark.implicits._

  val processStreamDf = streamDf
    .as[(Timestamp, Long)]
    .map(x => Record(x._1, x._2))
    .withColumn("batchId",
      (col("value") / 10).cast("int"))
    .groupBy("batchId")
    .agg(avg("value").as("averageValue"))


  processStreamDf.writeStream
    .outputMode(OutputMode.Complete())
    .format("console")
    .option("truncate", "false")
    .start()
    .awaitTermination()
}
