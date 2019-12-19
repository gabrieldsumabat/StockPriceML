package data.stocktimeseries.AlphaVantage

import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

object AlphaVantageCsvApi {
  val API_KEY = "6DXFTEKUH9EHJIYM"

   private def downloadTimeSeriesText(function: String, symbol:String, interval:String): String = {
    var BASE_URL_STRING = s"https://www.alphavantage.co/query?function=$function&symbol=$symbol&apikey=$API_KEY&datatype=csv&outputsize=full"
    if (function==TimeSeriesFunctions.INTRADAY){
      BASE_URL_STRING += s"&interval=$interval"
    }
    val content = scala.io.Source.fromURL(BASE_URL_STRING)
    val csv = content.mkString
    content.close()
    csv
  }

  private def downloadTechnicalIndicatorText(function: String, symbol:String, interval:String,time_period:Int,series_type:String): String = {
    val BASE_URL_STRING = s"https://www.alphavantage.co/query?function=$function&symbol=$symbol&interval=$interval&time_period=$time_period&series_type=$series_type&apikey=$API_KEY&datatype=csv"
    val content = scala.io.Source.fromURL(BASE_URL_STRING)
    val csv = content.mkString
    content.close()
    csv
  }

  def getStockTimeSeriesCsv(symbol: String, timeSeriesFunction: String = TimeSeriesFunctions.DAILY,interval:String = TimeSeriesIntradayIntervalEnum.fifteenMinutes): String = {
    val filePath = s"./csv/${symbol}_$timeSeriesFunction.csv"
    val csv = AlphaVantageCsvApi.downloadTimeSeriesText(timeSeriesFunction,symbol,interval)
    Files.newBufferedWriter(Paths.get(filePath),Charset.forName("UTF-8")).write(csv)
    filePath
  }

  def getTechicalIndicatorCsv(function: String, symbol:String, interval:String, time_period:Int, series_type:String): String ={
    val filePath = s"./csv/${symbol}_$function.csv"
    val csv = AlphaVantageCsvApi.downloadTechnicalIndicatorText(function,symbol,interval,time_period,series_type)
    Files.newBufferedWriter(Paths.get(filePath),Charset.forName("UTF-8")).write(csv)
    filePath
  }
}
