import data.stocktimeseries.AlphaVantage.{AlphaVantageCsvApi, TechnicalIndicatorIntervalEnum, TechnicalIndicatorSeriesTypeEnum, TimeSeriesFunctions, TimeSeriesIntradayIntervalEnum}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DecimalType, TimestampType}
import org.apache.spark.sql.functions._
import org.knowm.xchart.{SwingWrapper, XYChart, XYChartBuilder}
import org.knowm.xchart.XYSeries.XYSeriesRenderStyle
import org.knowm.xchart.style.Styler
import org.knowm.xchart.style.Styler.LegendPosition

import scala.collection.mutable


object IntradayModeler {
  val spark: SparkSession = SparkSession.builder().appName("Stock Predictor").master("local[*]").getOrCreate()

  def readHistoricalCsv(symbol: String,timeSeries:String, interval: String = TimeSeriesIntradayIntervalEnum.fiveMinutes): DataFrame = {
    spark.read.schema(TimeSeriesFunctions.getSchema).option("header",value = true)
      .csv(AlphaVantageCsvApi.getStockTimeSeriesCsv(symbol,timeSeries,interval))
  }

  def readTechnicalCsv(function: String, symbol:String, interval:String, time_period:Int, series_type:String): DataFrame ={
        spark.read.option("header",value = true)
          .csv(AlphaVantageCsvApi.getTechicalIndicatorCsv(function, symbol,interval,time_period,series_type))
          .withColumn("time",col("time").cast(TimestampType)).withColumnRenamed("time","timestamp")
  }

  def buildIntradayDf(symbol: String, indicators:List[String],interval:String, timeperiod:Int, seriesType:String=TechnicalIndicatorSeriesTypeEnum.CLOSE): DataFrame = {
    val timeSeriesDf = readHistoricalCsv(symbol,TimeSeriesFunctions.getSeriesFromInterval(interval))
    var apiLimitCounter = 1
    var intradayDF = indicators.foldLeft(mutable.ListBuffer[DataFrame]())(op = (dataFrameList, indicator) => {
      apiLimitCounter += 1
      if (apiLimitCounter == 5) {
        //Limited to 5 API calls per minute
        Thread.sleep(60030)
        apiLimitCounter = 0
      }
      dataFrameList += readTechnicalCsv(indicator, symbol, interval, timeperiod, seriesType)
    }).foldLeft(timeSeriesDf)((a:DataFrame,b:DataFrame)=> a.join(b,"timestamp"))
    intradayDF.columns.filter(!timeSeriesDf.columns.contains(_)).foreach(colName => {
      intradayDF = intradayDF.withColumn(colName, col(colName).cast(DecimalType(14, 6)))
    })
    intradayDF.orderBy(asc("timestamp"))
  }

  def vectorizeDataframe(indicators:Array[String],df:DataFrame): DataFrame ={
    val assembler = new VectorAssembler()
      .setInputCols(indicators)
      .setOutputCol("features")
    assembler.transform(df)
  }

  def getPredictedDataFrame(featureDf:DataFrame,targetCol:String): DataFrame ={
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
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

  def displayChart(x:Array[Double], target:Array[Double], predicted:Array[Double]): Unit = {
    val chart = new XYChartBuilder().width(800).height(600)
      .title(getClass.getSimpleName).xAxisTitle("Time").yAxisTitle("Price").build()
    chart.getStyler.setLegendPosition(LegendPosition.InsideNW)
    chart.getStyler.setDefaultSeriesRenderStyle(XYSeriesRenderStyle.Line)
    chart.getStyler.setYAxisLabelAlignment(Styler.TextAlignment.Right)
    chart.getStyler.setPlotMargin(0)
    chart.getStyler.setPlotContentSize(.95)
    chart.addSeries("Target", x, target)
    chart.addSeries("Predicted", x, predicted)
    new SwingWrapper[XYChart](chart).displayChart()
  }


  def main(args: Array[String]): Unit = {
    val symbol = "VIX"
    val indicators = List("SMA", "EMA", "VWAP", "MACD", "STOCH", "RSI", "ADX", "BBANDS", "OBV")

    //Build Feature Dataframe
    val intradayDf = buildIntradayDf(symbol, indicators, TechnicalIndicatorIntervalEnum.fiveMinutes, 20, TechnicalIndicatorSeriesTypeEnum.CLOSE)
    val cols = intradayDf.columns.filter(!_.matches("(timestamp|open|close|low|high)"))
    val intradayVectorizedDf = vectorizeDataframe(cols, intradayDf)

    //Machine Learning Algorithm
    val targetCol = "close"
    val predictions = getPredictedDataFrame(intradayVectorizedDf,targetCol)
    predictions.select("prediction", targetCol, "features").show(25,truncate = false)
    val evaluator = new RegressionEvaluator()
      .setLabelCol(targetCol)
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)

    //Graph Generated Model
    val uniformTime = (0 until predictions.count().toInt).map(_.toDouble).toArray
    val target = predictions.select(targetCol).collect.map(row => row.getDecimal(0).doubleValue())
    val predicted = predictions.select("prediction").collect.map(row => row.getDouble(0))
    displayChart(uniformTime,target,predicted)
  }
}