package chapter2
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}


object exampleJoins extends App{

  val spark = SparkSession.builder()
    .appName("ReviewAnalyzer")
    .master("local[*]")
    .getOrCreate()

  spark.conf.set("spark.sql.session.timeZone", "UTC")

  def read(file: String): DataFrame = {
    spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(file)
  }

  val reviewsDf = read("src/main/resources/employee_reviews/employee_reviews.csv")
  val departmentsDf = read("src/main/resources/departments/departments.csv")

  val joinedDf = reviewsDf
    .join(
      departmentsDf,
      reviewsDf.col("Department") === departmentsDf.col("dept_code"))
    .select(
      "Title",
      "Department",
      "dept_name")


  joinedDf
    .show()


  val reviewsSchema = reviewsDf.schema
  val departmentSchema = departmentsDf.schema

  def readStream(directory: String, schema: StructType): DataFrame = {
    spark
      .readStream
      .option("header", "true")
      .schema(schema)
      .csv(directory)
  }

  val reviewsStreamDf = readStream(
    "src/main/resources/employee_reviews",
    reviewsSchema)


  val departmentsStreamDf = readStream(
    "src/main/resources/departments",
    departmentSchema)


  reviewsStreamDf.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()


  departmentsStreamDf.writeStream
    .format("console")
    .outputMode("append")
    .start()
    .awaitTermination()

//  val joinedStreamDf = reviewsStreamDf
//    .join(
//      departmentsDf,
//      reviewsStreamDf.col("Department") === departmentsDf.col("dept_code"))
//    .select("Title", "Department", "dept_name")
//
//
//  val joinQuery = joinedStreamDf
//    .writeStream
//    .format("console")
//    .outputMode("append")
//    .start()
//
//  joinQuery.awaitTermination()
}
