package utils

import java.text.SimpleDateFormat
import java.util.Date

/**
  *
  */
object DateTimeUtil {

  val hourUnit = 3600000
  val dayUnit = 86400000
  val defaultFullFormat = "yyyy-MM-dd HH:mm:ss"
  val defaultDateFormat = "yyyy-MM-dd"

  def Today: String = {
    Today(defaultDateFormat)
  }

  def Today(format: String): String = {

    val sdf = new SimpleDateFormat(format)

    val currentTimestamp = System.currentTimeMillis()

    val date = new Date(currentTimestamp)

    return sdf.format(date)

  }

  def lastOffset(offset: Long): String = {
    lastOffset(defaultFullFormat, offset)
  }

  def lastOffset(format: String, offset: Long): String = {

    val sdf = new SimpleDateFormat(format)

    val currentTimestamp = System.currentTimeMillis() - offset

    val date = new Date(currentTimestamp)

    return sdf.format(date)

  }

  def lastHourDate(current:String): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd-HH")
    val date = sdf.parse(current).getTime - hourUnit
    sdf.format(new Date(date))
  }

  def twoHourAgoDate(current: String) : String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd-HH")
    val date = sdf.parse(current).getTime - hourUnit * 2
    sdf.format(new Date(date))
  }

  def rightOffset(offset: Long): String = {
    rightOffset(defaultFullFormat, offset)
  }

  def rightOffset(format: String, offset: Long): String = {

    val sdf = new SimpleDateFormat(format)

    val currentTimestamp = System.currentTimeMillis() + offset

    val date = new Date(currentTimestamp)

    return sdf.format(date)

  }

  def toDate(time: Long): String = {
    toDate(time, defaultFullFormat)
  }

  def toDate(time: Long, format: String): String = {

    val sdf = new SimpleDateFormat(format)

    val date = new Date(time)

    sdf.format(date)
  }

}
