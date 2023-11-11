package chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}



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

object Task3 extends App with Context {

  override val appName: String = "rate"

  val streamDf = spark
    .readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load

  import spark.implicits._

  val processStreamDf = streamDf
    .as[(Timestamp, Long)]
    .groupByKey( x => 1 )
    .mapGroupsWithState(GroupStateTimeout.NoTimeout())(
      updateAverage
    )


  processStreamDf.writeStream
    .outputMode(OutputMode.Update())
    .format("console")
    .option("truncate", "false")
    .start()
    .awaitTermination()

  case class RunningCount(count: Long, sum: Long)

  def updateAverage(
                     key: Int,
                     elements: Iterator[(Timestamp, Long)],
                     state: GroupState[RunningCount]
                   ): Double = {
    val batchSize = 10

    val previousState = if (state.exists) state.get else RunningCount(0, 0)

    var totalRecordCount = previousState.count
    var totalValue = previousState.sum

    var newRecordsCount = 0
    var newValuesSum = 0L

    while (elements.hasNext && newRecordsCount < batchSize) {
      val (timestamp, value) = elements.next()
      newRecordsCount += 1
      newValuesSum += value
    }

    totalRecordCount += newRecordsCount
    totalValue += newValuesSum

    val newState = RunningCount(totalRecordCount, totalValue)
    state.update(newState)

    if (totalRecordCount >= batchSize) {
      val average = totalValue.toDouble / totalRecordCount
      state.remove()
      average
    } else 0
  }
}
