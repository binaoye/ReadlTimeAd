/**
  * 项目名称：simpleanalysis
  * 项目描述：
  * 包 名 称：com.juxin.util
  * 版 本 号：0.0.1
  * 作    者：王  亚
  * 创建日期：2017/3/21 13:24
  *
  * Copyright @ 北京炬鑫网络科技有限公司 2017. All rights reserved.
  */

package utils

import java.io.{PrintWriter, StringWriter}

object ThrowablePrint {

  def getStackTrace(e: Throwable): String = {

    val sw = new StringWriter()

    e.printStackTrace(new PrintWriter(sw))

    sw.toString
  }

  def printStackTrace(e: Throwable): Unit = {

    val motd =
      """
        |              ┏┓      ┏┓
        |            ┏┛┻━━━┛┻┓
        |            ┃      ☃      ┃
        |            ┃  ┳┛  ┗┳  ┃
        |            ┃      ┻      ┃
        |            ┗━┓      ┏━┛
        |                ┃      ┗━━━┓
        |                ┃  神兽保佑    ┣┓
        |                ┃　永无BUG！   ┏┛
        |                ┗┓┓┏━┳┓┏┛
        |                  ┃┫┫  ┃┫┫
        |                  ┗┻┛  ┗┻┛
      """.stripMargin

    println(motd)

    println("==========Exception Start============================")

    println(s"异常消息:${e.getMessage}")

    val sw = new StringWriter()

    e.printStackTrace(new PrintWriter(sw))

    println(s"异常堆栈:\n${sw.toString}")

    println("==========Exception End==============================")

  }

}
