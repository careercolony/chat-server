package com.mj.chat.model

import java.util.Date

//mongoDB schema
sealed trait BaseMongoObj {
  var _id: String
}

case class User(var _id: String,
                login: String,
                nickname: String,
                password: String,
                gender: Int,
                avatar: String,
                lastLogin: Long = 0,
                loginCount: Int = 0,
                sessionsStatus: List[SessionStatus] = List(),
                dateline: Long = System.currentTimeMillis())
    extends BaseMongoObj

case class SessionStatus(sessionid: String, newCount: Int)

case class Session(var _id: String,
                   createuid: String,
                   ouid: String,
                   var sessionName: String,
                   var sessionIcon: String,
                   sessionType: Int,
                   publicType: Int,
                   usersStatus: List[UserStatus] = List(),
                   lastMsgid: String = "",
                   lastUpdate: Long = System.currentTimeMillis(),
                   dateline: Long = System.currentTimeMillis())
    extends BaseMongoObj

case class Mute(var _id: String, from: String, to: String) extends BaseMongoObj

case class UserStatus(uid: String, online: Boolean)

case class Message(var _id: String,
                   uid: String,
                   sessionid: String,
                   msgType: String,
                   content: String = "",
                   fileName: String = "",
                   fileType: String = "",
                   fileid: String = "",
                   thumbid: String = "",
                   dateline: Long = System.currentTimeMillis())
    extends BaseMongoObj

case class Online(var _id: String, uid: String, dateline: Date = new Date())
    extends BaseMongoObj

//mongoDB update result
case class UpdateResult(n: Int, errmsg: String)

////user and session token info
case class UserToken(uid: String, nickname: String, avatar: String)

case class SessionToken(sessionid: String,
                        sessionName: String,
                        sessionIcon: String)
