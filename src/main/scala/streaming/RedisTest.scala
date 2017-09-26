package streaming

import com.redis
import com.redis.RedisClient
import org.apache.avro.test.specialtypes.value
import streaming.CosumeKafka.{redisExpirationDate, _}

/**
  * Created by root on 2017/8/31.
  */
object RedisTest {
  val redisServer = "192.168.1.49"
//  val redisServer = "127.0.0.1"
  val redisPort = 6379
  val redisDBIndex = 99
  val redisPassword = Option("ABCD!1qaz@2wsx")
  val redisTimeout = 2000
  val redisExpirationDate = 2592000
  def main(args: Array[String]): Unit = {

//    val redisClient = new RedisClient(redisServer, redisPort, redisDBIndex,Option("ABCD!1qaz@2wsx") , redisTimeout)
    val redisClient = new RedisClient(redisServer,6379,redisDBIndex)
    val key = "test_1"
    val value = "100"
    redisClient.set(key, value, false, redis.Seconds(redisExpirationDate))

  }

}
