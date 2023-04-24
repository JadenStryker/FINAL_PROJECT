import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{VectorAssembler, MinMaxScaler}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.DenseMatrix
import org.apache.spark.ml.feature.{VectorAssembler, MinMaxScaler}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.functions.udf
import org.apache.spark.mllib.stat.Statistics
object ML {
def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark: SparkSession = SparkSession.builder()
    .appName("ML")
    .master("local[*]")
    .getOrCreate()

    // Read data from HDFS (or any other supported source)
    val inputDataFrame: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("hdfs://nyu-dataproc-m/user/jjs815_nyu_edu/final_project/swing/part-00000-cb6dc92a-7b7b-4786-8ec9-2b306caa7145-c000.csv")

    
    def normalize(df: DataFrame, inputCols: Array[String], outputCol: String): DataFrame = {
        val assembler = new VectorAssembler().setInputCols(inputCols).setOutputCol("features")
        val dataWithFeatures = assembler.transform(df)
        
        val scaler = new MinMaxScaler().setInputCol("features").setOutputCol(outputCol)
        val scalerModel = scaler.fit(dataWithFeatures)
        
        scalerModel.transform(dataWithFeatures).drop("features")
}

    // Set the target column name
    val target = "heart"

    // Get feature column names (excluding the target column)
    val featureColumns = Array("medcare_full_cov_amt", "Population", "CrimeRatePer100K", "Max AQI")

    // Normalize the data using Min-Max scaling
    val normalizedDataFrame = normalize(inputDataFrame, featureColumns, "scaledFeatures")

        // Prepare the data for linear regression
    val assembler = new VectorAssembler()
    .setInputCols(Array("scaledFeatures"))
    .setOutputCol("features")

    val dataForRegression = assembler.transform(normalizedDataFrame).select("features", target)

    // Train the linear regression model
    val lr = new LinearRegression().setLabelCol(target)
    val lrModel = lr.fit(dataForRegression)

    // Get training summary from the trained linear regression model
    val trainingSummary = lrModel.summary

    // Print Mean Squared Error (MSE)
    println(s"Mean Squared Error (MSE): ${trainingSummary.meanSquaredError}")



        def addTop10PercentileDummy(df: DataFrame, columnName: String, dummyColumnName: String): DataFrame = {
    // Calculate the 90th percentile value of the column
    val percentile90 = df.stat.approxQuantile(columnName, Array(0.9), 0.001).head

    // Define a UDF to create the dummy variable
    val top10PercentileDummy = udf((value: Double) => if (value >= percentile90) 1 else 0)

    // Add the dummy variable to the DataFrame
    df.withColumn(dummyColumnName, top10PercentileDummy(col(columnName)))
}
    val columnName = "heart" // The column name you want to base the dummy variable on
    val dummyColumnName = "heart_dummy" // The name of the new dummy variable column

    val dfWithDummy = addTop10PercentileDummy(inputDataFrame, columnName, dummyColumnName)
    val normalizedDataFrame = normalize(dfWithDummy, featureColumns, "scaledFeatures")


    val assembler = new VectorAssembler()
    .setInputCols(Array("scaledFeatures"))
    .setOutputCol("features")

    val dataForLOG = assembler.transform(normalizedDataFrame).select("features", target)

    // Train the logistic regression model
    val log = new LogisticRegression().setLabelCol("heart_dummy")
    val logModel = log.fit(dataForLOG)
        // Stop Spark session
    val predictions = logModel.transform(dataFordataForLOGRegression)

    // Get the coefficients (betas)
    val coefficients = logModel.coefficients
    println(s"Coefficients (Betas): $coefficients")

    // Create evaluators
    val binaryEvaluator = new BinaryClassificationEvaluator().setLabelCol(target)
    val multiClassEvaluator = new MulticlassClassificationEvaluator().setLabelCol(target)

    // Calculate and print F1 score, recall, and precision
    val f1Score = multiClassEvaluator.setMetricName("f1").evaluate(predictions)
    val recall = multiClassEvaluator.setMetricName("recall").evaluate(predictions)
    val precision = multiClassEvaluator.setMetricName("precision").evaluate(predictions)

    println(s"F1 Score: $f1Score")
    println(s"Recall: $recall")
    println(s"Precision: $precision")
    spark.stop()
}
}
