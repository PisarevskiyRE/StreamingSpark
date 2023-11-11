package chapter3

import chapter3.StateGroups.{Measurement, Record, StateRecord}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}

object StateGroups extends App{

  case class Record(deviceId: String, temperature: Double)

  case class StateRecord(deviceId: String, recordCount: Int, totalTemp: Double)

  case class Measurement(deviceId: String, recordCount: Int, totalTemp: Double, avgTemp: Double)


  val spark = SparkSession.builder()
    .appName("StateGroup")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._


  val streamDs = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    .load()
    .as[String]
    .map( line => {
      val rec = line.split(",")
      Record(rec(0), rec(1).toDouble)
    })

  val avgTempStreamDs = streamDs
    .groupByKey( x => x.deviceId)
    .mapGroupsWithState(
      GroupStateTimeout.NoTimeout())(
      updateAvgTemp
    )

  avgTempStreamDs
    .writeStream
    .format("console")
    .outputMode(OutputMode.Update())
    .start()
    .awaitTermination()


  def updateAvgTemp(key: String,
                    elements: Iterator[Record],
                    state: GroupState[StateRecord]): Measurement = {

    val previousState = {
      if (state.exists) state.get
      else StateRecord(key, 0, 0.0)
    }

    val currentCount: (Int, Double) = elements
      .foldLeft((0, 0.0)) { (currentData, record) =>
        val (currentCount, currentTemp) = currentData
        (currentCount + 1, currentTemp + record.temperature)
      }

    val (recordCount, tempSum) = currentCount

    val totalRecordCount = previousState.recordCount + recordCount
    val totalTemp = previousState.totalTemp + tempSum

    val newState = StateRecord(
      key,
      totalRecordCount,
      totalTemp
    )

    state.update(newState)

    Measurement(
      key,
      totalRecordCount,
      totalTemp,
      totalTemp / totalRecordCount
    )

  }
}

