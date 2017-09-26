package nlp

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.collection.JavaConversions._

class JcsegResult {

  val code = 0
  val data = new JcsegResultData()

  def getWordsArray(): String = {

    var rs = ""

    if (null != data && data.list.size() > 0) {

      for (w: JcsegResultDataItem <- data.list) {

        rs += w.word + " "

      }

    }

    return rs.trim

  }

  override def toString: String = {

    val mapper = new ObjectMapper()

    mapper.registerModule(DefaultScalaModule)

    return mapper.writeValueAsString(this)

  }

}
