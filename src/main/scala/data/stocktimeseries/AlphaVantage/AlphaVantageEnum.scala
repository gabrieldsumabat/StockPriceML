package data.stocktimeseries.AlphaVantage

import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType, StructField, StructType, TimestampType}

object TimeSeriesFunctions extends Enumeration {
  val INTRADAY = "TIME_SERIES_INTRADAY"
  val DAILY = "TIME_SERIES_DAILY"
  val DAILY_ADJUSTED = "TIME_SERIES_DAILY_ADJUSTED"
  val WEEKLY = "TIME_SERIES_WEEKLY"
  val WEEKLY_ADJUSTED = "TIME_SERIES_WEEKLY_ADJUSTED"
  val MONTHLY = "TIME_SERIES_MONTHLY"
  val MONTHLY_ADJUSTED = "TIME_SERIES_MONTHLY_ADJUSTED"

  def getSeriesFromInterval(series:String): String = {
    series match  {
      case "daily" => "TIME_SERIES_DAILY_ADJUSTED"
      case "weekly" => "TIME_SERIES_WEEKLY_ADJUSTED"
      case "monthly" => "TIME_SERIES_MONTHLY_ADJUSTED"
      case _ => "TIME_SERIES_INTRADAY"
    }
  }

  def getSchema: StructType = {
    StructType(Array(
      StructField("timestamp", TimestampType, nullable = false),
      StructField("open", DecimalType(14, 6), nullable = false),
      StructField("high", DecimalType(14, 6), nullable = false),
      StructField("low", DecimalType(14, 6), nullable = false),
      StructField("close", DecimalType(14, 6), nullable = false),
      StructField("volume", IntegerType, nullable = false)
    ))
  }

}

object TimeSeriesIntradayIntervalEnum extends Enumeration{
  val oneMinute = "1min"
  val fiveMinutes = "5min"
  val fifteenMinutes = "15min"
  val halfHour = "30min"
  val hourly = "60min"
}

object TechnicalIndicatorIntervalEnum extends Enumeration{
  val oneMinute = "1min"
  val fiveMinutes = "5min"
  val fifteenMinutes = "15min"
  val halfHour = "30min"
  val hourly = "60min"
  val daily = "daily"
  val weekly = "weekly"
  val monthly = "monthly"
}

object TechnicalIndicatorSeriesTypeEnum extends Enumeration{
  val CLOSE = "close"
  val OPEN = "open"
  val HIGH = "high"
  val LOW = "low"
}