package chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}



object Task3_b extends App with Context {

  override val appName: String = "rate"

  val streamDf = spark
    .readStream
    .format("rate")
    .option("rowsPerSecond", 1)
    .load

  import spark.implicits._

  val processStreamDf = streamDf
    .as[(Timestamp, Long)]
    .groupByKey( x => 1 )
    .flatMapGroupsWithState(OutputMode.Update(), GroupStateTimeout.NoTimeout())(
      updateAverage
    )


  processStreamDf.writeStream
    .outputMode(OutputMode.Update())
    .format("console")
    .option("truncate", "false")
    .start()
    .awaitTermination()

  case class RunningCount(count: Long, sum: Long)

  val BS: Int = 10

  def updateAverage(
                     key: Int,
                     elements: Iterator[(Timestamp, Long)],
                     state: GroupState[RunningCount]): Iterator[Double] = {

    val previousState: RunningCount = {
      if (state.exists) state.get
      else RunningCount(0,0)
    }


    val inputSorted: Seq[(Timestamp, Long)] = elements.toSeq.sortWith( (x,y) => x._1 > y._1 )

    val inputSum = inputSorted.map(x => x._2).sum
    val inputCount = inputSorted.length




    if ((previousState.count + inputCount) >= 10) {





      Iterator(
        previousState.sum + inputSorted.take((10 - previousState.count).toInt).map(x=>x._2).sum
      )

    }
    else Iterator()
  }
}

/*

8(50) + 5(10)  >= 10

8 + from 5(10 - 8)

 */
