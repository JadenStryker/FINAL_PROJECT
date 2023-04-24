import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType}

object Clean {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark = SparkSession.builder
      .appName("CountRecs")
      .getOrCreate()

    // Read command line arguments for CSV file path and separator
    if (args.length < 1) {
      println("Please provide the path to the CSV file.")
      System.exit(1)
    }
    val csvFilePath = args(0)
    val separator = ","

    // Read CSV file without considering the header
    val dfWithoutHeader = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .option("sep", separator)
      .csv(csvFilePath)

    // Get the header line as a string and print it
    val headerLine = dfWithoutHeader.first().mkString(separator)
    println(s"Header line: $headerLine")

    // Create a new schema based on the printed header line
    val newSchema = StructType(headerLine.split(separator).map(fieldName => StructField(fieldName.trim, StringType, nullable = true)))

    // Read CSV file with the new schema
    val df = spark.read
      .schema(newSchema)
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", separator)
      .csv(csvFilePath)

   
    // Count the number of records
    val rowCount = df.count()
    println(s"Row count: $rowCount")

    val cleanedDF = df.drop("LocationID", "LocationAbbr", "LocationDesc", "GeographicLevel", "DataSource",
    "Class", "Topic", "Data_Value_Unit", "Data_Value_Type", "Data_Value_Footnote_Symbol",
    "Data_Value_Footnote", "Confidence_limit_Low", "Confidence_limit_High", "StratificationCategory1",
    "StratificationCategory2", "StratificationCategory3", "TopicID", "X_long", "Y_lat")

    val noNullRows = cleanedDF.na.drop("any", Seq("Year", "Data_Value", "Stratification2", "Stratification1", "Stratification3"))


    noNullRows.write.mode("overwrite").option("header", "true").csv("hdfs://nyu-dataproc-m/user/jjs815_nyu_edu/hw7/output")

    // Stop the Spark session
    spark.stop()
  }
}

