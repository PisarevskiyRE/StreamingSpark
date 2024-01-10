package chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}

import scala.annotation.tailrec


object Task3_b extends App with Context {

  case class Record(timeStamp: Timestamp, value: Long)

  override val appName: String = "rate"

  object DEFAULT{
    val sliceCnt: Int = 10
    val rowsPerSecond: Int = 15
  }

  val streamDf = spark
    .readStream
    .format("rate")
    .option("rowsPerSecond", DEFAULT.rowsPerSecond)
    .load

  import spark.implicits._

  val processStreamDf = streamDf
    .as[(Timestamp, Long)]
    .map(rec => rec._2)
    .groupByKey( x => 1)
    .flatMapGroupsWithState(OutputMode.Update(),GroupStateTimeout.NoTimeout())(
      updateAverage
    )

  processStreamDf.writeStream
    .outputMode(OutputMode.Update())
    .format("console")
    .option("truncate", "false")
    .start()
    .awaitTermination()

  def updateAverage(  key: Int,
                      elements: Iterator[Long],
                      state: GroupState[Seq[Long]]): Iterator[String] = {

    val previousState: Seq[Long] = {
      if (state.exists) state.get
      else Seq()
    }

    val allElements: Seq[Long] = previousState ++ elements.toSeq.sortWith( (a,b) => a < b)

    // так как чисел может прилететь сколько угодно много
    // сделал рекурсию
    def getTopRows(elems: Seq[Long], cnt: Int): (Seq[Long], String) = {
      @tailrec
      def loop(x: Seq[Long], accumulator: String): (Seq[Long], String) = {
        if (x.size < cnt) (x, accumulator)
        else {
          val elemsForAvg = x.take(cnt)
          val newState = x.slice(cnt, x.size)
          loop(
            newState
            ,accumulator +
              "Для чисел => " +
              elemsForAvg.toString() +
              " Среднее значение => " +
              (elemsForAvg.sum / cnt).toString
          )
        }

      }
      loop(elems, "")
    }

    if (allElements.size >= DEFAULT.sliceCnt) {

      val newState = getTopRows(allElements, DEFAULT.rowsPerSecond)

      state.update(newState._1)
      Iterator( newState._2 )

    } else {
      state.update(allElements)
      Iterator("--- пока еще пусто ---")
    }
  }
}

