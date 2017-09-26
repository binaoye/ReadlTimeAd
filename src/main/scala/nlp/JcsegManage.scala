package nlp

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import utils.ThrowablePrint

import scalaj.http.Http


object JcsegManage {

//  val jcsegServer = "http://192.168.1.7:40082/tokenizer/master"
  val jcsegServer = "http://192.168.1.7:40082/tokenizer/juxin_purify_env"
//  val jcsegServer = "http://127.0.0.1:40082/tokenizer/juxin_purify_env"
  val httpTimeOut = 1000 * 120
  val jsonMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def getKeywords(content: String): String = {

    val rs = Http(jcsegServer)
      .param("text", content.replace(" ",""))
      .timeout(httpTimeOut, httpTimeOut)
      .asString

    try {

      val jcsegResult = jsonMapper.readValue(rs.body, classOf[JcsegResult])

      var wordArray = ""

      if (jcsegResult.code == 0 && jcsegResult.data.list.size() > 0) {

        wordArray = jcsegResult.getWordsArray()

      }

      return wordArray
    } catch {
      case e: Throwable => {
        println(rs)
        ThrowablePrint.printStackTrace(e)
        return ""
      }
    }
  }
}
