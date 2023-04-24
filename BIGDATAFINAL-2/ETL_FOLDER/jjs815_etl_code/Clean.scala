import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{col, lower}

object Clean{
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark = SparkSession.builder
      .appName("Clean")
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
      .option("multiLine", "true") // Read records that span multiple lines
      .option("ignoreLeadingWhiteSpace", "true") // Ignore leading white spaces in each field
      .option("ignoreTrailingWhiteSpace", "true") // Ignore trailing white spaces in each field
      .option("nullValue", "\"\"") // Treat empty double quotes as null values
      .csv(csvFilePath)

    // Drop the unnecessary columns
    val cleanedDF = df.drop("LocationID", "GeographicLevel", "DataSource",
      "Class", "Topic", "Data_Value_Unit", "Data_Value_Type", "Data_Value_Footnote_Symbol",
      "Data_Value_Footnote", "Confidence_limit_Low", "Confidence_limit_High", "StratificationCategory1",
      "StratificationCategory2", "Stratification2", "Stratification3", "StratificationCategory3", "Stratification1", "TopicID", "X_long", "Y_lat")



    // drop the percentage change rows. Not something I need
    // Drop rows with null values in the specified columns
    val noNullRows = cleanedDF.na.drop("any", Seq("Year", "Data_Value", "LocationAbbr", "LocationDesc"))



    val renamedColumnsDF = noNullRows
      .withColumnRenamed("LocationAbbr", "state")
      .withColumnRenamed("LocationDesc", "county")


    val dfWithLowercaseColumns = renamedColumnsDF
    .withColumn("county", lower(col("county")))
    .withColumn("state", lower(col("state")))

    // drop the percentage change rows. Not something I need
    val filteredRows = dfWithLowercaseColumns
    .filter(!col("Data_Value").contains("%") && !col("Year").contains("-"))

    val filteredYearsBetween = filteredRows.filter(col("Year").between(2008, 2019))

    val dfNoDuplicates = filteredYearsBetween.dropDuplicates()

    dfNoDuplicates.coalesce(1).write.mode("overwrite").option("header", "true").csv("hdfs://nyu-dataproc-m/user/jjs815_nyu_edu/final_project/output")

    // Stop the Spark session
    spark.stop()
  }
}
