import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Analysis {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark = SparkSession.builder
      .appName("Analysis")
      .getOrCreate()

    // Read command line arguments for CSV file path and separator
    if (args.length < 1) {
      println("Please provide the path to the CSV file.")
      System.exit(1)
    }
    val csvFilePath = args(0)
    val separator = if (args.length >= 2) args(1) else ","

    // Read CSV file from HDFS
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", separator)
      .option("multiLine", "true")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "true")
      .option("nullValue", "\"\"")
      .csv(csvFilePath)

    // Find distinct values in columns
    val columnsToFindDistinctValues = List("age_group", "race_group", "sex_group", "state", "county")

    columnsToFindDistinctValues.foreach { columnName =>
      val distinctValues = df.select(columnName).distinct()
      println(s"Distinct values in column '$columnName':")
      distinctValues.show()
    }

    // Stop the Spark session
    spark.stop()
  }
}