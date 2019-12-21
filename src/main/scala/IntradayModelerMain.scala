import data.stocktimeseries.AlphaVantage.{TechnicalIndicatorIntervalEnum, TechnicalIndicatorSeriesTypeEnum}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.DataFrame

object IntradayModelerMain extends IntradayModeler {
  def main(args: Array[String]): Unit = {
    val symbol = "VIX"
    val indicators = List("SMA", "EMA", "VWAP", "MACD", "STOCH", "RSI", "ADX", "BBANDS", "OBV")
    val targetCol = "close"
    val featureCol = "features"
    val predictionCol = "prediction"

    //Build Feature Dataframe
    val intradayDf = buildIntradayDf(symbol, indicators, TechnicalIndicatorIntervalEnum.fiveMinutes, 20, TechnicalIndicatorSeriesTypeEnum.CLOSE)
    val cols = intradayDf.columns.filter(!_.matches("(timestamp|close|low|high)"))
    val intradayVectorizedDf = vectorizeDataframe(cols, intradayDf,featureCol)
    //Machine Learning Algorithm
    val predictions = getPredictedDataFrame(intradayVectorizedDf,targetCol,featureCol)
    predictions.select(predictionCol, targetCol, featureCol).show(25,truncate = false)
    val evaluator = new RegressionEvaluator()
      .setLabelCol(targetCol)
      .setPredictionCol(predictionCol)
      .setMetricName("rmse")
    println("Root Mean Squared Error (RMSE) on test data = " + evaluator.evaluate(predictions))
    //Graph Generated Model
    val uniformTime = (0 until predictions.count().toInt).map(_.toDouble).toArray
    val target = predictions.select(targetCol).collect.map(row => row.getDecimal(0).doubleValue())
    val predicted = predictions.select(predictionCol).collect.map(row => row.getDouble(0))
    displayChart(uniformTime,target,predicted)
  }

  def getPredictedDataFrame(featureDf: DataFrame, targetCol: String, featureCol: String): DataFrame = {
    val featureIndexer = new VectorIndexer()
      .setInputCol(featureCol)
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(featureDf)
    val Array(trainingData, testData) = featureDf.randomSplit(Array(0.7, 0.3))
    // Train a RandomForest model.
    val rf = new RandomForestRegressor()
      .setLabelCol(targetCol)
      .setFeaturesCol("indexedFeatures")
    val pipeline = new Pipeline()
      .setStages(Array(featureIndexer, rf))
    val model = pipeline.fit(trainingData)
    model.transform(testData)
  }
}