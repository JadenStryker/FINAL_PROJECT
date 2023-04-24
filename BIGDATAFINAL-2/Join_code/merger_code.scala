
// val jaden_data = sc.textFile("final_code_etl/data/jaden_data.csv")

// jaden_data.take(5).foreach(println)

// val mateus_data = sc.textFile("final_code_etl/data/mateus_data.csv")

// mateus_data.take(5).foreach(println)

// val rahul_final_data = sc.textFile("final_code_etl/data/rahul_final_data.csv")

// rahul_final_data.take(5).foreach(println)

// val leo_data = sc.textFile("final_code_etl/data/leo_data.tsv")

// leo_data.take(5).foreach(println)

// val states_code_data = sc.textFile("final_code_etl/data/states.csv")

// states_code_data.take(5).foreach(println)


import org.apache.spark.sql._
import org.apache.spark.sql.functions._

def processRahulData(rahulPath: String, statesPath: String)(implicit spark: SparkSession): DataFrame = {
  // Read CSV files
  val rahulData = spark.read.option("header", "true").csv(rahulPath)
  val statesData = spark.read.option("header", "true").csv(statesPath)

  // Convert 'State' and 'Abbreviation' columns to lower case
  val lowerStatesData = statesData
    .withColumn("State", lower(col("State")))
    .withColumn("Abbreviation", lower(col("Abbreviation")))

  // Merge rahulData with statesData
  val mergedDF = rahulData.join(lowerStatesData, Seq("State"))

  // Add the new 'County' column
  val updatedDF = mergedDF
    .withColumn("Year", col("Year").cast("string"))
    .withColumn("County", concat(col("Year"), lit("_"), col("Abbreviation"), lit("_"), col("County")))

  // Drop 'State' column
  val rahulDataWithCode = updatedDF.drop("State")

  // Filter DataFrame based on the year range
  val rahulDataWithCodeRanged = rahulDataWithCode
    .filter((col("Year").cast("int") >= 2010) && (col("Year").cast("int") <= 2014))

  rahulDataWithCodeRanged
}

// Usage
val spark = SparkSession.builder().appName("Process Rahul Data").getOrCreate()

val rahulPath = "final_code_etl/data/rahul_final_data.csv"
val statesPath = "final_code_etl/data/states.csv"

val resultDF = processRahulData(rahulPath, statesPath)(spark)

resultDF.show(5)

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

def processMateusData(mateusPath: String)(implicit spark: SparkSession): DataFrame = {
  // Read CSV file
  val dataMateus = spark.read.option("header", "true").csv(mateusPath)

  // Filter rows based on 'BENE_GEO_LVL' column
  val dataMateusCounty = dataMateus.filter(col("BENE_GEO_LVL") === "County")

  // Replace '-' with '_' in the 'BENE_GEO_DESC' column
  val updatedDataMateusCounty = dataMateusCounty
    .withColumn("BENE_GEO_DESC", regexp_replace(col("BENE_GEO_DESC"), "-", "_"))

  // Filter DataFrame based on the year range (2010 to 2014)
  val dataMateusRanged = updatedDataMateusCounty
    .filter((col("YEAR").cast("int") >= 2010) && (col("YEAR").cast("int") <= 2014))

  // Add the new 'id' column
  val withIdColumn = dataMateusRanged
    .withColumn("YEAR", col("YEAR").cast("int").cast("string"))
    .withColumn("id", lower(concat(col("YEAR"), lit("_"), col("BENE_GEO_DESC"))))

  // Rename columns
  val renamedColumns = withIdColumn
    .withColumnRenamed("BENES_WTH_PTAPTB_CNT", "medcare_full_cov_amt")
    .withColumnRenamed("BENES_EVER_MA_CNT", "MA_enroll_amt")
    .withColumnRenamed("EVER_MA_PRTCPTN_RATE", "MA_full_cov_enroll_rate")

  // Drop unnecessary columns
  val finalDataMateus = renamedColumns.drop("YEAR", "BENE_GEO_LVL", "BENE_GEO_DESC", "BENE_GEO_CD", "BENE_AGE_LVL")

  finalDataMateus
}

// Usage
val spark = SparkSession.builder().appName("Process Mateus Data").getOrCreate()

val mateusPath = "final_code_etl/data/mateus_data.csv"

val resultDF = processMateusData(mateusPath)(spark)

resultDF.show(5)



import org.apache.spark.sql._
import org.apache.spark.sql.functions._

// New function to join the DataFrames
def joinMateusAndRahulData(rahulDF: DataFrame, mateusDF: DataFrame): DataFrame = {
  val mateusRahulDataMerged = rahulDF
    .join(mateusDF, rahulDF("County") === mateusDF("id"), "inner")

  mateusRahulDataMerged
}

// Usage
val spark = SparkSession.builder().appName("Process and Join Mateus and Rahul Data").getOrCreate()

val rahulPath = "final_code_etl/data/rahul_final_data.csv"
val statesPath = "final_code_etl/data/states.csv"
val mateusPath = "final_code_etl/data/mateus_data.csv"

val rahulData = processRahulData(rahulPath, statesPath)(spark)
val mateusData = processMateusData(mateusPath)(spark)

val joinedData = joinMateusAndRahulData(rahulData, mateusData)

joinedData.show(5)


import org.apache.spark.sql._
import org.apache.spark.sql.functions._

def processJadenData(jadenPath: String)(implicit spark: SparkSession): DataFrame = {
  val jadenData = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(jadenPath)

  val jadensDataRanged = jadenData
    .filter(col("Year") >= 2010 && col("Year") <= 2014)

  val jadensDataRangedNoDuplicates = jadensDataRanged
    .groupBy(col("Year"), col("state"), col("county"))
    .agg(mean(col("Data_Value")).alias("Data_Value"))
    .withColumn("id", concat(col("Year").cast("string"), lit("_"), col("state"), lit("_"), col("county")))

  jadensDataRangedNoDuplicates
}

// Usage
val spark = SparkSession.builder().appName("Process Jaden Data").getOrCreate()
val jadenPath = "final_code_etl/data/jaden_data.csv"

val jadenData = processJadenData(jadenPath)(spark)
jadenData.show(5)

def mergeJadenAndLeoData(jadenData: DataFrame, leoData: DataFrame): DataFrame = {
  val jadenLeoDataMerged = jadenData.join(leoData, jadenData("id") === leoData("KEY"), "inner")
    .drop("KEY")
    .withColumnRenamed("Data_Value", "heart")

  val newColumnOrder = Seq("id", "Year", "state", "county", "Population", "#Crimes", "Off0", "Off1", "Off2", "Off3", "CrimeRatePer100K", "heart")

  val jadenLeoDataMergedOrdered = jadenLeoDataMerged.select(newColumnOrder.map(col): _*)

  jadenLeoDataMergedOrdered
}

// reading leo data 
def readLeoData(filePath: String): DataFrame = {
  spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", "\t")
    .load(filePath)
}

val leoData = readLeoData("final_code_etl/data/leo_data.tsv")

val jadenLeoDataMerged = mergeJadenAndLeoData(jadenData, leoData)
jadenLeoDataMerged.show(5)

import org.apache.spark.sql.DataFrame

def mergeMateusRahulWithJadenLeo(mateusRahulData: DataFrame, jadenLeoData: DataFrame): DataFrame = {
  val fullMerge = mateusRahulData.join(jadenLeoData, "id")

  val cleanedFullMerge = fullMerge.drop("Year_x", "Abbreviation", "id", "Year_y", "county")

  cleanedFullMerge
}


val fullMergedData = mergeMateusRahulWithJadenLeo(joinedData, jadenLeoDataMerged)
fullMergedData.show(5)

val coalescedData = fullMergedData.coalesce(1)

// Write coalesced DataFrame as CSV file
coalescedData.write
  .format("csv")
  .option("header", "true")
  .mode("overwrite")
  .save("hdfs://nyu-dataproc-m/user/msa8779_nyu_edu/final_code_etl/output")