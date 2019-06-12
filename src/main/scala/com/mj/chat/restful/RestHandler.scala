package com.mj.chat.restful

import com.mj.chat.model.ChatMessage
import com.mj.chat.mongo.MongoLogic._
import com.mj.chat.tools.CommonUtils._
import play.api.libs.json._
import reactivemongo.bson._

import scala.concurrent.{ExecutionContext, Future}

object RestHandler {
  def registerUserCtl(
      login: String,
      nickname: String,
      password: String,
      repassword: String,
      gender: Int)(implicit ec: ExecutionContext): Future[JsObject] = {
    if (password != repassword) {
      Future {
        Json.obj(
          "uid" -> "",
          "errmsg" -> s"password and repassword must be same",
          "successmsg" -> ""
        )
      }
    } else {
      registerUser(login, nickname, password, gender).map {
        case (uid, errmsg) =>
          var successmsg = ""
          if (uid != "") {
            successmsg = "register user success, thank you for join us"
          }
          Json.obj(
            "uid" -> uid,
            "errmsg" -> errmsg,
            "successmsg" -> successmsg
          )
      }
    }
  }

  def createMuteCtl(uid: String, to: String)(
      implicit ec: ExecutionContext): Future[JsObject] = {
    insertMute(uid, to).map {
      case Some(errmsg) =>
        Json.obj(
          "sessionid" -> "",
          "errmsg" -> errmsg,
          "successmsg" -> "create mute failed"
        )
      case _ =>
        Json.obj(
          "sessionid" -> "",
          "errmsg" -> "",
          "successmsg" -> "create mute success"
        )
    }
  }

  def removeMuteCrl(uid: String, to: String)(
      implicit ec: ExecutionContext): Future[JsObject] = {
    removeMute(uid, to).map { updateResult =>
      if (updateResult.errmsg != "") {
        Json.obj(
          "errmsg" -> updateResult.errmsg,
          "successmsg" -> ""
        )
      } else {
        Json.obj(
          "errmsg" -> "",
          "successmsg" -> "remove mute success"
        )
      }
    }

  }

  def loginCtl(login: String, password: String)(
      implicit ec: ExecutionContext): Future[JsObject] = {
    if (password.length < 6) {
      Future {
        Json.obj(
          "uid" -> "",
          "errmsg" -> s"password must at least 6 characters",
          "successmsg" -> "",
          "userToken" -> ""
        )
      }
    } else {
      loginAction(login, password).map {
        case Some(uid) =>
          Json.obj(
            "uid" -> uid,
            "errmsg" -> "",
            "successmsg" -> "login in success"
          )
        case _ =>
          Json.obj(
            "uid" -> "",
            "errmsg" -> "user not exist or password not match",
            "successmsg" -> ""
          )
      }
    }
  }

  def logoutCtl(uid: String)(
      implicit ec: ExecutionContext): Future[JsObject] = {
    logoutAction(uid).map(res => Option(res.errmsg).filterNot(_.isEmpty)).map {
      case Some(errmsg) =>
        Json.obj(
          "errmsg" -> errmsg,
          "successmsg" -> ""
        )
      case _ =>
        Json.obj(
          "errmsg" -> "",
          "successmsg" -> "logout success"
        )
    }
  }

  def getUserInfoCtl(uid: String)(
      implicit ec: ExecutionContext): Future[JsObject] = {
    getUserInfo(uid).map(Option(_)).map {
      case Some(user) =>
        Json.obj(
          "errmsg" -> "",
          "successmsg" -> "get user info success",
          "userInfo" -> Json.obj(
            "uid" -> user._id,
            "nickname" -> user.nickname,
            "avatar" -> user.avatar,
            "gender" -> user.gender,
            "login" -> user.login,
            "lastLogin" -> timeToStr(user.lastLogin),
            "loginCount" -> user.loginCount
          )
        )
      case _ =>
        Json.obj(
          "errmsg" -> "user not exist",
          "successmsg" -> "",
          "userInfo" -> JsNull
        )
    }
  }

  def listSessionsCtl(uid: String, isPublic: Boolean)(
      implicit ec: ExecutionContext): Future[JsObject] = {
    listSessions(uid, isPublic)
      .map { sessionInfoList =>
        Future.sequence(
          sessionInfoList.map {
            case (session, sessionStatus) =>
              getSessionLastMessage(uid, session._id).map {
                case (sessionLast, messageLast, userLast) =>
                  var jsonMessage: JsValue = JsNull
                  if (messageLast != null && userLast != null) {
                    var content = messageLast.content
                    if (messageLast.thumbid != "") {
                      content = "send a [PHOTO]"
                    } else if (messageLast.fileid != "") {
                      content = "send a [FILE]"
                    }
                    jsonMessage = Json.obj(
                      "uid" -> userLast._id,
                      "nickname" -> userLast.nickname,
                      "avatar" -> userLast.avatar,
                      "msgType" -> messageLast.msgType,
                      "content" -> content,
                      "dateline" -> timeToStr(messageLast.dateline)
                    )
                  }
                  Json.obj(
                    "sessionid" -> session._id,
                    "createuid" -> session.createuid,
                    "ouid" -> session.ouid,
                    "sessionName" -> session.sessionName.take(30),
                    "sessionType" -> session.sessionType,
                    "sessionIcon" -> session.sessionIcon,
                    "publicType" -> session.publicType,
                    "lastUpdate" -> timeToStr(session.lastUpdate),
                    "dateline" -> timeToStr(session.dateline),
                    "newCount" -> sessionStatus.newCount,
                    "message" -> jsonMessage
                  )
              }
          }
        )
      }
      .flatMap(t => t)
      .map { sessions =>
        Json.obj(
          "errmsg" -> "",
          "sessions" -> sessions
        )
      }
  }

  def listJoinedSessionsCtl(uid: String)(
      implicit ec: ExecutionContext): Future[JsObject] = {
    listJoinedSessions(uid).map { sessionInfoList =>
      val sessions = sessionInfoList.map {
        case (session, sessionStatus) =>
          Json.obj(
            "sessionid" -> session._id,
            "createuid" -> session.createuid,
            "sessionName" -> trimUtf8(session.sessionName, 24),
            "sessionType" -> session.sessionType,
            "sessionIcon" -> session.sessionIcon,
            "publicType" -> session.publicType,
            "dateline" -> timeToStr(session.dateline),
            "lastUpdate" -> timeToStr(session.lastUpdate),
            "newCount" -> sessionStatus.newCount
          )
      }
      Json.obj(
        "errmsg" -> "",
        "sessions" -> sessions
      )
    }
  }

  def listMessagesCtl(
      uid: String,
      sessionid: String,
      page: Int = 1,
      count: Int = 10)(implicit ec: ExecutionContext): Future[JsObject] = {
    implicit val chatMessageWrites = Json.writes[ChatMessage]
    for {
      ret <- {
        listHistoryMessages(uid,
                            sessionid,
                            page,
                            count,
                            sort = document("dateline" -> -1)).map {
          case (errmsg, messageUsers) =>
            Json.obj(
              "errmsg" -> errmsg,
              "messages" -> messageUsers.reverse.map {
                case (message, user) =>
                  var suid = ""
                  var snickname = ""
                  var savatar = ""
                  if (user != null) {
                    suid = user._id
                    snickname = user.nickname
                    savatar = user.avatar
                  }
                  val chatMessage = ChatMessage(suid,
                                                snickname,
                                                savatar,
                                                message.msgType,
                                                message.content,
                                                message.fileName,
                                                message.fileType,
                                                message.fileid,
                                                message.thumbid,
                                                timeToStr(message.dateline))
                  Json.toJson(chatMessage)
              }
            )
        }
      }
    } yield {
      ret
    }
  }

  def getUserInfoByNameCtl(nickName: String)(
      implicit ec: ExecutionContext): Future[JsObject] = {
    getUserInfoByName(nickName).map { users =>
      if (users == null) {
        Json.obj(
          "errmsg" -> "user not exist",
          "successmsg" -> "",
          "userInfo" -> JsNull
        )
      } else {
        Json.obj(
          "errmsg" -> "",
          "successmsg" -> "get user info success",
          "userInfo" -> users.map { user =>
            Json.obj(
              "uid" -> user._id,
              "nickname" -> user.nickname,
              "avatar" -> user.avatar,
              "gender" -> user.gender,
              "dateline" -> timeToStr(user.dateline)
            )
          }
        )
      }
    }
  }

  def getJoinedUsersCtl(uid: String, sessionid: String)(
      implicit ec: ExecutionContext): Future[JsObject] = {

    getJoinedUsers(sessionid).map {
      case (session, users) =>
        if (session != null) {
          val onlineUsers = session.usersStatus
            .filter(_.online)
            .map(_.uid)
            .map { uid =>
              users.find(_._id == uid).orNull
            }
            .filter(_ != null)
          val offlineUsers = session.usersStatus
            .filterNot(_.online)
            .map(_.uid)
            .map { uid =>
              users.find(_._id == uid).orNull
            }
            .filter(_ != null)
          Json.obj(
            "errmsg" -> "",
            "onlineUsers" -> onlineUsers.map { user =>
              Json.obj(
                "uid" -> user._id,
                "nickname" -> user.nickname,
                "avatar" -> user.avatar
              )
            },
            "offlineUsers" -> offlineUsers.map { user =>
              Json.obj(
                "uid" -> user._id,
                "nickname" -> user.nickname,
                "avatar" -> user.avatar
              )
            }
          )
        } else {
          Json.obj(
            "errmsg" -> "session not exist",
            "onlineUsers" -> JsArray(),
            "offlineUsers" -> JsArray()
          )
        }
    }
  }

  def getPrivateSessionCtl(uid: String, ouid: String)(
      implicit ec: ExecutionContext): Future[JsObject] = {
    createPrivateSession(uid, ouid).map {
      case (sessionid, errmsg) =>
        Json.obj(
          "errmsg" -> errmsg,
          "sessionid" -> sessionid
        )
    }
  }

  def getSessionHeaderCtl(uid: String, sessionid: String)(
      implicit ec: ExecutionContext): Future[JsObject] = {
    getSessionHeader(uid, sessionid).map {
      case (session, sessionToken) =>
        if (session != null && sessionToken.sessionName != "") {
          Json.obj(
            "errmsg" -> "",
            "session" -> Json.obj(
              "sessionid" -> sessionid,
              "sessionName" -> sessionToken.sessionName,
              "sessionIcon" -> sessionToken.sessionIcon,
              "createuid" -> session.createuid,
              "ouid" -> session.ouid
            )
          )
        } else {
          Json.obj(
            "errmsg" -> "no privilege or session not exists",
            "session" -> JsNull
          )
        }
    }
  }

  def getSessionMenuCtl(uid: String, sessionid: String)(
      implicit ec: ExecutionContext): Future[JsObject] = {
    getSessionMenu(uid, sessionid).map {
      case (session, joined, editable) =>
        if (session == null) {
          Json.obj(
            "errmsg" -> "no privilege to get session menu",
            "session" -> JsNull
          )
        } else {
          Json.obj(
            "errmsg" -> "",
            "session" -> Json.obj(
              "sessionid" -> session._id,
              "sessionName" -> session.sessionName,
              "sessionIcon" -> session.sessionIcon,
              "createuid" -> session.createuid,
              "ouid" -> session.ouid,
              "joined" -> joined,
              "editable" -> editable
            )
          )
        }
    }
  }

  def getUserMenuCtl(uid: String)(
      implicit ec: ExecutionContext): Future[JsObject] = {
    getUserMenu(uid).map {
      case Some(ouser) =>
        Json.obj(
          "errmsg" -> "",
          "user" -> Json.obj(
            "uid" -> ouser._id,
            "nickname" -> ouser.nickname,
            "avatar" -> ouser.avatar,
            "gender" -> ouser.gender
          )
        )
      case _ =>
        Json.obj(
          "errmsg" -> "no privilege to get user menu",
          "user" -> JsNull
        )
    }
  }

}
