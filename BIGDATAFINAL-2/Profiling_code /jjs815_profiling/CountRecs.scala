import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType}

object CountRecs {
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
    //println(s"Header line: $headerLine")

    // Create a new schema based on the printed header line
    val newSchema = StructType(headerLine.split(separator).map(fieldName => StructField(fieldName.trim, StringType, nullable = true)))

    // Read CSV file with the new schema
    val df = spark.read
      .schema(newSchema)
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", separator)
      .csv(csvFilePath)

    //println("DataFrame schema:")
    //df.printSchema()
    //df.show(5)

    // Count the number of records
    val rowCount = df.count()
    println(s"Row count: $rowCount")

    // Example: Map the records to a key and a value, and count the number of records using map() function
    val mappedRecords = df.rdd.map(row => (row.getAs[String]("Year"), 1))
    val mappedRecordCount = mappedRecords.count()
    println(s"Mapped record count: $mappedRecordCount")

    // Example: Find the distinct values in each column
    val distinctColumn1Values = df.select("Year").distinct()
    val distinctColumn2Values = df.select("Data_Value").distinct()
    val distinctColumn3Values = df.select("Stratification2").distinct()
    val distinctColumn4Values = df.select("Stratification1").distinct()
    val distinctColumn5Values = df.select("Stratification3").distinct()

    println("Distinct values in columns:")
    distinctColumn1Values.show()
    distinctColumn2Values.show()
    distinctColumn3Values.show()
    distinctColumn4Values.show()
    distinctColumn5Values.show()

    // Stop the Spark session
    spark.stop()
  }
}

