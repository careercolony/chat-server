package com.mj.chat.model

import akka.actor.ActorRef
import com.mj.chat.tools.CommonUtils._

//akka stream message type
sealed trait WsMessageUp {
  val uid: String
}

case class WsTextUp(uid: String,
                    nickname: String,
                    avatar: String,
                    sessionid: String,
                    sessionName: String,
                    sessionIcon: String,
                    msgType: String,
                    content: String)
    extends WsMessageUp

//akka stream message type
sealed trait WsMessageDown

case class WsTextDown(uid: String,
                      nickname: String,
                      avatar: String,
                      sessionid: String,
                      sessionName: String,
                      sessionIcon: String,
                      msgType: String,
                      content: String,
                      dateline: String = timeToStr(System.currentTimeMillis()))
    extends WsMessageDown

//akka stream message type
case class UserOnline(actor: ActorRef) extends WsMessageDown

case object UserOffline extends WsMessageDown

//akka cluster message type
case class ClusterText(uid: String,
                       nickname: String,
                       avatar: String,
                       sessionid: String,
                       sessionName: String,
                       sessionIcon: String,
                       msgType: String,
                       content: String,
                       dateline: String = timeToStr(System.currentTimeMillis()))
    extends WsMessageDown

//client message type
case class ChatMessage(uid: String,
                       nickname: String,
                       avatar: String,
                       msgType: String,
                       content: String,
                       fileName: String,
                       fileType: String,
                       fileid: String,
                       thumbid: String,
                       dateline: String = timeToStr(System.currentTimeMillis()))
    extends WsMessageDown

case class PushMessage(uid: String,
                       nickname: String,
                       avatar: String,
                       sessionid: String,
                       sessionName: String,
                       sessionIcon: String,
                       msgType: String,
                       content: String,
                       fileName: String,
                       fileType: String,
                       fileid: String,
                       thumbid: String,
                       dateline: String = timeToStr(System.currentTimeMillis()))
    extends WsMessageDown
