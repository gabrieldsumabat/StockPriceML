package data.stocktimeseries.AlphaVantage

import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

object AlphaVantageCsvApi {
  val API_KEY: String = System.getenv("AV_API_KEY")

  def getStockTimeSeriesCsv(symbol: String, function: String, interval: Option[String] = Option.empty, time_period: Option[Int] = Option.empty, series_type: Option[String] = Option.empty): String = {
    val filePath = s"./csv/${symbol}_$function.csv"
    val intervalArg = interval match {
      case None => Option.empty
      case Some(_) => Some(AlphaVantageArg("interval",interval))
    }
    val timePeriodArg = time_period match {
      case None => Option.empty
      case Some(_) => Some(AlphaVantageArg("time_period",Option(time_period.get.toString)))
    }
    val seriesTypeArg = series_type match {
      case None => Option.empty
      case Some(_) => Some(AlphaVantageArg("series_type",series_type))
    }
    val inputArgArray:Array[Option[AlphaVantageArg]] = Array(
      Some(AlphaVantageArg("symbol",Some(symbol))),
      Some(AlphaVantageArg("function",Some(function))),
      intervalArg,
      timePeriodArg,
      seriesTypeArg
    )
    val csv = AlphaVantageCsvApi.downloadTimeSeriesText(inputArgArray)
    Files.newBufferedWriter(Paths.get(filePath), Charset.forName("UTF-8")).write(csv)
    filePath
  }

  private def downloadTimeSeriesText(args: Array[Option[AlphaVantageArg]]): String = {
    var BASE_URL_STRING = s"https://www.alphavantage.co/query?apikey=$API_KEY&datatype=csv&outputsize=full"
    args.foreach { argOption => { argOption match {
        case None =>
        case Some(value) => BASE_URL_STRING += s"&${value.ARG_NAME}=${value.ARG_VALUE.get}"
      }}}
    val content = scala.io.Source.fromURL(BASE_URL_STRING)
    val csv = content.mkString
    content.close()
    csv
  }

  case class AlphaVantageArg(ARG_NAME: String, ARG_VALUE: Option[String])
}

