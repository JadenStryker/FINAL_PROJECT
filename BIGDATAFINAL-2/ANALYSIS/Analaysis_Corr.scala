import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.DenseMatrix
import org.apache.spark.ml.feature.{VectorAssembler, MinMaxScaler}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.mllib.stat.Statistics.corr
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.DataFrame


object DataFrameFilterColumns {
def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark: SparkSession = SparkSession.builder()
    .appName("DataFrameFilterColumns")
    .master("local[*]")
    .getOrCreate()

    // Read data from HDFS (or any other supported source)
    val inputDataFrame: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("hdfs://nyu-dataproc-m/user/jjs815_nyu_edu/final_project/readyforanalysis/full_merge.csv")

    // Define the list of columns to keep
    val columnsToKeep: List[String] = List("County","heart", "medcare_full_cov_amt", "Population", "CrimeRatePer100K", "state", "Max AQI")

    // Filter the DataFrame by the list of columns to keep
    val filteredDataFrame: DataFrame = inputDataFrame.select(columnsToKeep.map(col): _*)
    val dfs = filteredDataFrame

    def extractYear(x: String): Int = {
    x.split("_")(0).toInt
    }

    def extractCounty(x: String): String = {
    x.split("_")(2)
    }

    val extractYearUDF = udf(extractYear _)
    val extractCountyUDF = udf(extractCounty _)

    val dfsWithYear: DataFrame = dfs.withColumn("Year", extractYearUDF(col("County")))
    val dfsWithYearAndCounty: DataFrame = dfsWithYear.withColumn("county", extractCountyUDF(col("County")))
    val dfsRenamed: DataFrame = dfsWithYearAndCounty.withColumnRenamed("County", "key")


    def pearsonCorr(df: DataFrame): Unit = {
  // Get columns to compute correlation, excluding the specified columns
  val columnsToCompute = df.columns.filterNot(col => List("heart", "key", "county", "state").contains(col))

  // Convert the 'heart' column to RDD[Double]
  val rddHeart = df.select("heart").rdd.map(row => row.getDouble(0))

  // Compute and print the correlations
  for (col <- columnsToCompute) {
    val rddFeature = df.select(col).rdd.map(row => row.getDouble(0))
    val correlation = Statistics.corr(rddFeature, rddHeart, "pearson")
    println(s"$col: $correlation")
  }
}



    pearsonCorr(dfsRenamed)

    // Save the filtered DataFrame to HDFS (or any other supported sink)
    // filteredDataFrame.write
    // .mode("overwrite")
    // .option("header", "true")
    // .csv("hdfs:///path/to/your/output/data_filtered.csv")

    // Stop Spark session
    spark.stop()
}
}
