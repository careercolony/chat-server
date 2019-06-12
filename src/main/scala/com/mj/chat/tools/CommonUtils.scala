package com.mj.chat.tools

import java.security.MessageDigest
import java.text.SimpleDateFormat

import org.joda.time.DateTime
import play.api.libs.json.{JsArray, JsNumber, JsString, JsValue}

object CommonUtils {

  case class CustomException(message: String = "", cause: Throwable = null)
      extends Exception(message, cause)

  def consoleLog(logType: String, msg: String): Unit = {
    val timeStr = new DateTime().toString("yyyy-MM-dd HH:mm:ss")
    println(s"[$logType] $timeStr: $msg")
  }

  def getJsonString(json: JsValue,
                    field: String,
                    default: String = ""): String = {
    val ret = (json \ field).getOrElse(JsString(default)).as[String]
    ret
  }

  def sha1(str: String) =
    MessageDigest
      .getInstance("SHA-1")
      .digest(str.getBytes)
      .map("%02x".format(_))
      .mkString

  def isEmail(email: String): Boolean = {
    """(?=[^\s]+)(?=(\w+)@([\w\.]+))""".r.findFirstIn(email).isDefined
  }

  def timeToStr(time: Long = System.currentTimeMillis()) = {
    val sdf = new SimpleDateFormat("MM-dd HH:mm:ss")
    sdf.format(time)
  }

  def trimUtf8(str: String, len: Int) = {
    var i = 0
    var strNew = ""
    str.foreach { ch =>
      if (i < len) {
        strNew = strNew + ch
      }
      var charLen = ch.toString.getBytes.length
      if (charLen > 2) {
        charLen = 2
      }
      i = i + charLen
    }
    strNew
  }

}
