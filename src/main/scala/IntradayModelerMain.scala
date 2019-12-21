import data.stocktimeseries.AlphaVantage.{TechnicalIndicatorIntervalEnum, TechnicalIndicatorSeriesTypeEnum}
import org.apache.spark.ml.evaluation.RegressionEvaluator

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
}