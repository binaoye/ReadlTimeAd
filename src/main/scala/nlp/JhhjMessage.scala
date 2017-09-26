package nlp

import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.Row
import utils.{DateTimeUtil, ThrowablePrint}

import scala.collection.JavaConversions._

class JhhjMessage {

  var date = ""
  var uid_max = 0L
  var uid_min = 0L
  val content = new util.ArrayList[JhhjMessageItem]()
  val probs = new util.ArrayList[NameValue]()
  var event_type = new NameValue()
  var key = ""

  def fil(messages: util.ArrayList[Row]): Unit = {

    try {

      val mapper = new ObjectMapper()
      //mapper.registerModule(DefaultScalaModule)

      val rowListIt = messages.iterator()
      while (rowListIt.hasNext) {

        val row = rowListIt.next()

        //chat_key,chat_msgid,chat_time,chat_uid,chat_touid,chat_img,chat_voice,chat_content,chat_words,chat_label,chat_d
        key = row.get(0).asInstanceOf[String]
        val uid = row.get(3).asInstanceOf[Long]
        val touid = row.get(4).asInstanceOf[Long]
        val label = row.get(9).asInstanceOf[String]
        val time = row.get(2).asInstanceOf[Long]
        val msgid = row.get(1).asInstanceOf[Long]
        val d = row.get(10).asInstanceOf[Long]

        if (uid > uid_max) uid_max = uid
        if (touid > uid_max) uid_max = touid
        if (uid < uid_max) uid_min = uid
        if (touid < uid_max) uid_min = touid

        date = DateTimeUtil.toDate(time * 1000, DateTimeUtil.defaultDateFormat)

        val messageItem = new JhhjMessageItem()
        messageItem.mid = msgid

        val mapList = mapper.readValue(label, classOf[util.ArrayList[Any]])

        for (map <- mapList) {
          val linkMap = map.asInstanceOf[util.LinkedHashMap[String, Any]]
          val nameValue = new NameValue()
          nameValue.name = linkMap.get("name").asInstanceOf[String]
          nameValue.value = linkMap.get("value")
          messageItem.probs.add(nameValue)
        }

        content.add(messageItem)
      }

      //找出每个类别概率最大的元素
      val maxProbs = new util.HashMap[String, Double]()
      for (msg: JhhjMessageItem <- content) {
        for (p <- msg.probs) {
          if (maxProbs.containsKey(p.name) == false) {
            maxProbs.put(p.name, p.value.asInstanceOf[Double])
          } else {
            val mapItemValue = maxProbs.get(p.name)
            if (p.value.asInstanceOf[Double] > mapItemValue) {
              maxProbs.put(p.name, p.value.asInstanceOf[Double])
            }
          }
        }
      }

      //所有聊天消息中,最大概率的N个标签
      val keyIt = maxProbs.keySet().iterator()
      while (keyIt.hasNext) {
        val nameValue = new NameValue()
        nameValue.name = keyIt.next()
        nameValue.value = maxProbs.get(nameValue.name)
        probs.add(nameValue)
      }

      //最大概率的1个标签
      for (p: NameValue <- probs) {
        if (p.value.asInstanceOf[Double] > event_type.value.asInstanceOf[Double]) {
          event_type = p
        }
      }

    } catch {
      case e: Throwable => {
        ThrowablePrint.printStackTrace(e)
      }
    }
  }

  override def toString: String = {

    val mapper = new ObjectMapper()

    mapper.registerModule(DefaultScalaModule)

    return mapper.writeValueAsString(this)
  }

  class JhhjMessageItem {
    var mid = 0L
    var probs = new util.ArrayList[NameValue]()
  }

}
