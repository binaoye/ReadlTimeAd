package nlp

import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.Row
import utils.{DateTimeUtil, ThrowablePrint}

import scala.collection.JavaConversions._

class JhhjContextMessage {

  var key = ""

  var mids = ""

  var probs = new util.ArrayList[NameValue]()

  var probsMap = new util.HashMap[String, Any]()

  var event_type = new NameValue()

  var uid_min = 0L

  var uid_max = 0L

  var date = ""

  def fil(row: Row): Unit = {

    //Row(chat_key, chat_mids, chat_content, chat_words, chat_label, chat_time, chat_uid_min, chat_uid_max)

    try {

      key = row.getString(0)
      mids = row.getString(1)

      date = DateTimeUtil.toDate(row.getLong(5) * 1000, DateTimeUtil.defaultDateFormat)
      uid_min = row.getLong(6)
      uid_max = row.getLong(7)

      val label = row.getString(4)
      val mapper = new ObjectMapper()
      val mapList = mapper.readValue(label, classOf[util.ArrayList[Any]])

      for (map <- mapList) {

        val linkMap = map.asInstanceOf[util.LinkedHashMap[String, Any]]

        val nameValue = new NameValue()
        nameValue.name = linkMap.get("name").asInstanceOf[String]
        nameValue.value = linkMap.get("value")

        probs.add(nameValue)
        probsMap.put(nameValue.name, nameValue.value)

        if (nameValue.value.asInstanceOf[Double] > event_type.value.asInstanceOf[Double]) {
          event_type.name = nameValue.name
          event_type.value = nameValue.value
        }

      }


    } catch {
      case e: Throwable => ThrowablePrint.printStackTrace(e)
    }
  }


  override def toString: String = {

    val mapper = new ObjectMapper()

    mapper.registerModule(DefaultScalaModule)

    return mapper.writeValueAsString(this)
  }

}
