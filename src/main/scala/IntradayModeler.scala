import data.stocktimeseries.AlphaVantage.{AlphaVantageCsvApi, TechnicalIndicatorSeriesTypeEnum, TimeSeriesFunctions, TimeSeriesIntradayIntervalEnum}
import org.apache.spark.ml.Pipeline
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


class IntradayModeler {
  val spark: SparkSession = SparkSession.builder().appName("Stock Predictor").master("local[*]").getOrCreate()

  def readHistoricalCsv(symbol: String, timeSeries: String, interval: String = TimeSeriesIntradayIntervalEnum.fiveMinutes): DataFrame = {
    spark.read.schema(TimeSeriesFunctions.getSchema).option("header", value = true)
      .csv(AlphaVantageCsvApi.getStockTimeSeriesCsv(symbol, timeSeries, Some(interval)))
  }

  def readTechnicalCsv(function: String, symbol: String, interval: String, time_period: Int, series_type: String): DataFrame = {
    spark.read.option("header", value = true)
      .csv(AlphaVantageCsvApi.getStockTimeSeriesCsv(symbol, function, Some(interval), Some(time_period), Some(series_type)))
      .withColumn("timestamp", col("time").cast(TimestampType))
  }

  def buildIntradayDf(symbol: String, indicators: List[String], interval: String, timeperiod: Int, seriesType: String = TechnicalIndicatorSeriesTypeEnum.CLOSE): DataFrame = {
    val timeSeriesDf = readHistoricalCsv(symbol, TimeSeriesFunctions.getSeriesFromInterval(interval))
    var apiLimitCounter = 1
    var intradayDF = indicators.foldLeft(mutable.ListBuffer[DataFrame]())(op = (dataFrameList, indicator) => {
      apiLimitCounter += 1
      if (apiLimitCounter == 5) {
        //Limited to 5 API calls per minute
        Thread.sleep(60030)
        apiLimitCounter = 0
      }
      dataFrameList += readTechnicalCsv(indicator, symbol, interval, timeperiod, seriesType)
    }).foldLeft(timeSeriesDf)((a: DataFrame, b: DataFrame) => a.join(b, "timestamp"))
    intradayDF.columns.filter(!timeSeriesDf.columns.contains(_)).foreach(colName => {
      intradayDF = intradayDF.withColumn(colName, col(colName).cast(DecimalType(14, 6)))
    })
    intradayDF.orderBy(asc("timestamp"))
  }

  def vectorizeDataframe(indicators: Array[String], df: DataFrame, featureCol: String): DataFrame = {
    val assembler = new VectorAssembler()
      .setInputCols(indicators)
      .setOutputCol(featureCol)
    assembler.transform(df)
  }

  def displayChart(x: Array[Double], target: Array[Double], predicted: Array[Double]): Unit = {
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

