package streaming

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.redis.RedisClient
import nlp.{JcsegManage, JhhjMessage, NameValue}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.{Tokenizer, Word2VecModel}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.ThrowablePrint

import scalaj.http.Http

/**
  * Created by root on 2017/9/4.
  */
object streamingTest {
  val kafkaTopic = Seq("flume.juxin.yuanfenba")
  val kafkaGroup = "flume.consumer.juxin.yuanfenba.hdfs.jhhj"
  //  val kafkaGroup = "flume.consumer.juxin.yuanfenba.hdfs.jhhj"
  val kafkaServer = "jx-1-11:9092,jx-1-14:9092,jx-1-3:9092"
  val redisServer = "192.168.1.49"
  val redis_recent = 50
  val redis_hist = 51
  val redis_gift = 52
  val redisPort = 6379
  val redisDBIndex = 0
  val redisPassword = Option("ABCD!1qaz@2wsx")
  val redisTimeout = 2000
  val redisExpirationDate = 2592000
  val sparkTimeSpan = 10
  //  val word2VecModel = "F:\\Ad\\offline\\w2vmodel"
  val word2VecModel = "/juxin/data-ml-model/purify-env/ads/word2vec-model"
  val lrModelPath = "/juxin/data-ml-model/purify-env/ads/lr-model"
  //  val lrModelPath = "F:\\Ad\\offline\\train_lr_storePath"
  val jsonMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  val httpPostUrl = "http://www.xiaoyoo.cn:20003/badthing/getChar"
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    //    System.setProperty("hadoop.home.dir", "F:/hadoop-2.5.2")
    val conf = new SparkConf()
      .setAppName("realtime-addetect")
    //      .setMaster("local")
    val ssc = new StreamingContext(conf, Seconds(sparkTimeSpan))
    //ssc.checkpoint("spark-checkpoint")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaServer,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> kafkaGroup,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](kafkaTopic, kafkaParams)
    val allData = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      consumerStrategy
    ).map(record => {
      val msg: String = record.value()
      val jsonMapper = new ObjectMapper()
      //      println(msg)
      try{
        val jsonNode: JsonNode = jsonMapper.readTree(msg.substring(msg.indexOf("{"), msg.length))
        val topic = jsonNode.get("topic").asText()
        val message = jsonNode.get("message").toString
        (topic, message)
      }catch {
        case e: Throwable => ThrowablePrint.printStackTrace(e)
          ("","")
      }

      //      println(s"${topic},${message}")

    })

    val GiftSend = allData.filter(_._1.equals("GiftSend")).foreachRDD(r=>{
      r.foreachPartition(f = fn =>{
        val redisClient_gift = new RedisClient(redisServer, redisPort, redis_gift, redisPassword, redisTimeout)
        while(fn.hasNext){
          try {
            val ne: (String, String) = fn.next()
            val jsonMapper = new ObjectMapper()
            val kv: JsonNode = jsonMapper.readTree(ne._2)
            val uid = kv.get("uid").asLong()
            val to_uid = kv.get("receive_uid").asLong()
            val key = if (uid < to_uid) s"${uid}-${to_uid}" else s"${to_uid}-${uid}"
            val key_gift = s"juxin:realtime:adsdetect:histgift:${key}"
            if (redisClient_gift.exists(key_gift)) {
              val ct = redisClient_gift.get(key_gift).get.toInt
              redisClient_gift.set(key_gift, ct)

            } else {
              redisClient_gift.set(key_gift, 1)
            }
          } catch {
            case e:Throwable => ThrowablePrint.printStackTrace(e)
          }


        }
      })
    })
    //    allData.count()
    //
    println("过滤聊天记录")
    val chatSend = allData
      .filter(_._1.equals("ChatSend"))
      .map(kvs=>{
        println(kvs._1)
        val jsonMapper = new ObjectMapper()
        val kv: JsonNode = jsonMapper.readTree(kvs._2)
        //        val kvs =
        val time = kv.get("time").asLong()
        val uid = kv.get("uid").asLong()
        val to_uid = kv.get("touid").asLong()
        val fromkf = kv.get("fromkf").asInt()
        val tokf = kv.get("tokf").asInt()
        val chat_type = kv.get("chat_type").asInt()
        val context = kv.get("content").asText()
        val msgid = kv.get("msgid").asLong()
        var content = ""
        var img = ""
        var voice = ""
        var d = 0L
        var key = ""
        if (uid >= to_uid) {
          key = to_uid.toString + uid.toString
        } else {
          key = uid.toString + to_uid.toString
        }
        if (chat_type == 2) {
          val contentJsonString = kv.get("content").asText("")
          if (null != contentJsonString && contentJsonString.equals("") == false) {
            val mapper = new ObjectMapper()
            val contentJsonNode = mapper.readTree(contentJsonString)
            val it = contentJsonNode.fieldNames()
            while (it.hasNext) {
              val fieldName = it.next()
              if (fieldName.equalsIgnoreCase("mct")) {
                content = contentJsonNode.get("mct").asText("").trim
              } else if (fieldName.equalsIgnoreCase("voice")) {
                voice = contentJsonNode.get("voice").toString.trim
              } else if (fieldName.equalsIgnoreCase("img")) {
                img = contentJsonNode.get("img").asText("").trim
              } else if (fieldName.equalsIgnoreCase("d")) {
                d = contentJsonNode.get("d").longValue()
              }

            }
          }
        }
        Tuple12.apply(chat_type, msgid, key, time, uid, to_uid, content, img, voice, fromkf, tokf, d)
      }).filter(r => r._10 == 0 && r._11 == 0 && r._1 == 2 )//更新近期聊天句数记录

    println("更新历史与近期聊天句数redis库")

    chatSend.foreachRDD(r=>r.foreachPartition( f = fn =>{
      //        val redisClient = new RedisClient(redisServer, redisPort, redisDBIndex, redisPassword, redisTimeout)
      println("创建redis连接")
      val redisClient_recent = new RedisClient(redisServer, redisPort, redis_recent, redisPassword, redisTimeout)
      val redisClient_history = new RedisClient(redisServer, redisPort, redis_hist, redisPassword, redisTimeout)
      while(fn.hasNext){
        val ne: (Int, Long, String, Long, Long, Long, String, String, String, Int, Int, Long) = fn.next
        val currentTimestamp = System.currentTimeMillis()
        val keyDate = TimestampToDate(currentTimestamp, "yyyy:MM:dd:HH:mm")
        val key_recent = s"juxin:realtime:adsdetect:recentchat:${ne._5}-${ne._6}"  //两小时记录详情
        val key_hist = s"juxin:realtime:adsdetect:histchat:${ne._5}-${ne._6}"      //历史聊天句数
        val value_recent = Set(ne._4)
        redisClient_recent.sadd(key_recent,value_recent)
        println(key_hist)
        if(redisClient_history.exists(key_hist)){
          val chat_count = redisClient_history.get(key_hist).get.toInt + 1
          redisClient_history.set(key_hist,chat_count)
        }else{
          redisClient_history.set(key_hist,1)
        }

      }
    }
    )
    )

    println("获取近期与历史聊天句数")
    val chatHist = chatSend.foreachRDD(r=>{

      //      val spark = SessionMaker.make("local")
      val spark = SparkSession.builder().getOrCreate()
      println("加载word2vec模型")
      var tok = new Tokenizer()
        .setInputCol("words")
        .setOutputCol("text")

      val w2cModel = Word2VecModel.load(word2VecModel)

      println("加载预测模型")
      val logModel = LogisticRegressionModel.load(lrModelPath)
      println("创建session")
      val rowRDD: RDD[(String, Int, Long, String, Long, Long, Long, String, String, String, Int, Int, Long, Int, Int, Int, Int, Int)] = r.map(line=>{

        try {
          val redisClient_recent = new RedisClient(redisServer, redisPort, redis_recent, redisPassword, redisTimeout)
          val redisClient_history = new RedisClient(redisServer, redisPort, redis_hist, redisPassword, redisTimeout)
          val redisClient_gift = new RedisClient(redisServer, redisPort, redis_gift, redisPassword, redisTimeout)

          val key_recent_u_t = s"juxin:realtime:adsdetect:recentchat:${line._5}-${line._6}"
          val key_recent_t_u = s"juxin:realtime:adsdetect:recentchat:${line._5}-${line._6}"
          val key_hist_u_t = s"juxin:realtime:adsdetect:histchat:${line._5}-${line._6}"
          val key_hist_t_u = s"juxin:realtime:adsdetect:histchat:${line._5}-${line._6}"
          val gift_key = if(line._5 < line._6) s"${line._5}${line._6}" else s"${line._6}${line._5}"

          val content = line._7
          println(content)
          val words = JcsegManage.getKeywords(content)
          val count_hist_u_t = if (redisClient_history.exists(key_hist_u_t)) redisClient_history.get(key_hist_u_t).get.toInt else 0
          val count_hist_t_u = if (redisClient_history.exists(key_hist_t_u)) redisClient_history.get(key_hist_t_u).get.toInt else 0
          val count_recent_u_t = if (redisClient_recent.exists(key_recent_u_t)) redisClient_recent.smembers(key_recent_u_t).toIterator.size else 0
          val count_recent_t_u = if (redisClient_recent.exists(key_recent_t_u)) redisClient_recent.smembers(key_recent_t_u).toIterator.size else 0
          val count_hist_gift = if(redisClient_gift.exists(gift_key)) redisClient_gift.get(gift_key).get.toInt else 0
          Tuple18(words, line._1, line._2, line._3, line._4, line._5, line._6, line._7, line._8, line._9, line._10, line._11, line._12, count_hist_u_t, count_hist_t_u, count_recent_u_t, count_recent_t_u ,count_hist_gift)
        } catch {
          case  e :Throwable => ThrowablePrint.printStackTrace(e)
            Tuple18("",0,0L,"",0L,0L,0L,"","","",0,0,1L,0,0,0,0,0)

        }
      })
//      rowRDD.count()
      val rowDF = spark.createDataFrame(rowRDD).toDF("words","chat_type", "msgid", "key", "time", "uid", "to_uid", "content", "img", "voice", "fromkf", "tokf", "d","count_hist_u_t","count_hist_t_u","count_recent_u_t","count_recent_t_u","count_gift")
        .filter("words!=''").filter("msgid!=0")
      println("调用模型进行预测")
      println("关键词转数组格式")
      var wordArrayDataFrame = tok.transform(rowDF)

      println("转换词向量")
      val modelData = w2cModel.transform(wordArrayDataFrame)
//      modelData.show()

      println("使用模型进行预测")
      val preds = logModel.transform(modelData)
//      preds.printSchema()
//      preds.show()

      val pd = preds.rdd
        .map(row=>{
          try {
            val chat_msgid = row.getAs[Long]("msgid")
            val chat_time = row.getAs[Long]("time")
            val chat_uid = row.getAs[Long]("uid")
            val chat_touid = row.getAs[Long]("to_uid")
            val chat_key = if(chat_uid>chat_touid) chat_touid.toString + chat_uid.toString else chat_uid.toString + chat_touid.toString
            val chat_img = row.getAs[String]("img")
            val chat_voice = row.getAs[String]("voice")
            val chat_content = row.getAs[String]("content")
            val chat_words = row.getAs[String]("words")
            val chat_d = row.getAs[Long]("d")
            val chatLabelList = new util.ArrayList[NameValue]()
            val nameValue = new NameValue()
            val len = chat_content.length
            val count_hist_u_t = row.getAs[Int]("count_hist_u_t")
            val count_hist_t_u = row.getAs[Int]("count_hist_t_u")
            val count_recent_u_t = row.getAs[Int]("count_recent_u_t")
            val count_recent_t_u = row.getAs[Int]("count_recent_t_u")
            val hist_all = count_hist_t_u + count_hist_u_t
            val recent_all = count_recent_t_u + count_recent_u_t
            val hist_gift = row.getAs[Int]("count_gift")
            /*
            根据以下规则调整概率
            调整规则：近期不常联系且历史也不常联系，则使用模型预测值，否则使用预测概率中的较小值
            定义-常联系：聊天句子数>4且不为单向且无历史礼物
            定义-单向：总句数>4且一方句数<=2
             */
            val use_pred = if(recent_all<4&&hist_all<6&&hist_gift==0 && len>10) true else false
            val pb = if(use_pred) row.getAs[DenseVector]("probability").toArray.toList.last else row.getAs[DenseVector]("probability").toArray.sorted.toList.head
            val prob = if(pb>0.5) pb + 0.3 * (1-pb) else 0.5 * pb
            nameValue.name = "21"
            nameValue.value = BigDecimal(prob * 100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
            chatLabelList.add(nameValue)
            val chat_label = jsonMapper.writeValueAsString(chatLabelList)
            Row(chat_key, chat_msgid, chat_time, chat_uid, chat_touid, chat_img, chat_voice, chat_content, chat_words, chat_label, chat_d)
          } catch {
            case e:Throwable => ThrowablePrint.printStackTrace(e)
              Row.empty
          }
        })

      //      saveToHdfs(spark,pd)
      sendToApi(pd)
    }
    )





    ssc.start()
    ssc.awaitTermination()
  }
  def TimestampToDate(timestamp: Long, format: String): String = {
    val sdf = new SimpleDateFormat(format)
    val date = new Date(timestamp)
    return sdf.format(date)
  }

  def sendToApi(df: RDD[Row]): Unit = {

    println("发送数据到API净化环境接口")

    df.repartition(20).groupBy(r => r.get(0).asInstanceOf[String]).foreach(fn => {
      try {
        val it = fn._2.iterator

        val rowList = new util.ArrayList[Row]()

        while (it.hasNext) {
          rowList.add(it.next())
        }

        val messageDto = new JhhjMessage()

        messageDto.fil(rowList)

        val jsonDataString = messageDto.toString

        val rs = Http(httpPostUrl)
          .postForm(Seq("data" -> jsonDataString))
          .timeout(12000, 12000)
          .asString

        if (rs.code != 200) {
          println(s"Response:${rs.code} ${rs.body} Request:${jsonDataString}")
        }

      } catch {
        case e: Throwable =>  ThrowablePrint.printStackTrace(e)
      }

    })
  }

  def saveToHdfs(spark: SparkSession, rdd: RDD[Row]): Unit = {


    val rand = Math.random()
    val totalReportPath = s"F:\\Ad\\realtime\\${rand}"
    //    val totalReportPath = "/user/xiaoke/readtime/ads/"
    println(s"保存数据到HDFS目录:${totalReportPath}")

    //chat_key, chat_msgid, chat_time, chat_uid, chat_touid, chat_img, chat_voice, chat_content, chat_words, chat_label, chat_d
    val schema = StructType(Array(
      StructField("chat_key", StringType, true),
      StructField("chat_msgid", LongType, true),
      StructField("chat_time", LongType, true),
      StructField("chat_uid", LongType, true),
      StructField("chat_touid", LongType, true),
      StructField("chat_img", StringType, true),
      StructField("chat_voice", StringType, true),
      StructField("chat_content", StringType, true),
      StructField("chat_words", StringType, true),
      StructField("chat_label", StringType, true),
      StructField("chat_d", LongType, true)
    ))

    val totalDF = spark.createDataFrame(rdd, schema)

    totalDF.repartition(1).write.mode(SaveMode.Append).json(totalReportPath)

  }

  def rddToDataFrame(spark: SparkSession, rdd: RDD[Row]): DataFrame = {

    val wordTextSchema = StructType(Array(
      StructField("msgid", LongType, true),
      StructField("key", StringType, true),
      StructField("time", LongType, true),
      StructField("uid", LongType, true),
      StructField("touid", LongType, true),
      StructField("img", StringType, true),
      StructField("voice", StringType, true),
      StructField("content", StringType, true),
      StructField("words", StringType, true),
      StructField("d", LongType, true)
    ))

    spark.createDataFrame(rdd, wordTextSchema).filter("words!=''").filter("msgid!=0")
  }

  def delIcons(str: String) : String = {
    val stopWords = "白眼\n微笑\n尴尬\n调皮\n发呆\n呲牙\n偷笑\n撇嘴\n大哭\n愉快\n惊讶\n调皮\n流泪\n白眼\n大哭\n害羞\n囧\n色\n睡\n撇嘴\n傲慢\n抓狂\n偷笑\n得意\n闭嘴\n吐"
      .split("\n")
    var ans = str
    for(i <- 0 until stopWords.length){
      ans = ans.replace(stopWords(i),"")
    }
    ans.replace("[","").replace("]","")
  }

}
