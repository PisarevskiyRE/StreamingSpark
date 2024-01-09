package chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}

import scala.annotation.tailrec


object Task3_b extends App with Context {

  case class Record(timeStamp: Timestamp, value: Long)


  override val appName: String = "rate"

  val streamDf = spark
    .readStream
    .format("rate")
    .option("rowsPerSecond", 15)
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



  val cnt: Int = 10


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
    def getTopNRows(elems: Seq[Long]): (Seq[Long], String) = {

      @tailrec
      def loop(x: Seq[Long], accumulator: String): (Seq[Long], String) = {

        if (x.size < 10) (x, accumulator)
        else {
          val elemsForAvg = x.take(10)
          val newState = x.slice(10, x.size)

          loop(
            newState
            ,accumulator + "Для чисел => " +elemsForAvg.toString()+ "<= Среднее значение " + (elemsForAvg.sum / 10).toString + "\\n"
          )
        }
      }
      loop(elems, "")
    }

    if (allElements.size >= 10) {

      val newState = getTopNRows(allElements)


      state.update(newState._1)
      Iterator( newState._2 )

    } else {

      state.update(allElements)

      Iterator("---")
    }
  }
}

