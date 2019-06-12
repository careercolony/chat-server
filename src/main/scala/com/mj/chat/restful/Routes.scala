package com.mj.chat.restful

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.mj.chat.model._
import com.mj.chat.mongo.MongoLogic._
import com.mj.chat.restful.RestHandler._
import com.mj.chat.tools.JsonRepo._
import com.mj.chat.websocket.{ChatSession, PushSession}
import play.api.libs.json.Json

import scala.concurrent.ExecutionContext

object Routes {
  //init create mongodb collection
  createUsersCollection()
  createSessionsCollection()
  createMessagesCollection()
  createMutesCollection()
  createOnlinesCollection()

  def routeLogic(implicit ec: ExecutionContext,
                 system: ActorSystem,
                 materializer: ActorMaterializer) = {
    routeWebsocket ~
      routeUserRegister ~
      routeUserLogin ~
      routeUserLogout ~
      routeGetUserInfo ~
      routeGetUserMenu ~
      routeGetUserInfoByName ~
      routeListSessions ~
      routeListJoinedSessions ~
      routeGetJoinedUsers ~
      routeGetPrivateSession ~
      routeGetSessionHeader ~
      routeGetSessionMenu ~
      routeCreateMute ~
      routeRemoveMute ~
      routeListMessages
  }

  def routeWebsocket(implicit ec: ExecutionContext,
                     system: ActorSystem,
                     materializer: ActorMaterializer): Route = {
    get {
      //use for chat service
      path("ws-chat") {
        val chatSession = new ChatSession()
        handleWebSocketMessages(chatSession.chatService)
        //use for push service
      } ~ path("ws-push") {
        val pushSession = new PushSession()
        handleWebSocketMessages(pushSession.pushService)
      }
    }
  }

  def routeUserRegister(implicit ec: ExecutionContext): Route =
    path("api" / "registerUser") {
      post {
        entity(as[RegisterDto]) { dto =>
          complete {
            registerUserCtl(dto.login,
                            dto.nickname,
                            dto.password,
                            dto.repassword,
                            dto.gender) map { json =>
              HttpEntity(ContentTypes.`application/json`, Json.stringify(json))
            }
          }
        }
      }
    }

  def routeCreateMute(implicit ec: ExecutionContext): Route =
    path("api" / "createMute") {
      post {
        entity(as[MuteDto]) { muteDto =>
          complete {
            createMuteCtl(muteDto.from, muteDto.to) map { json =>
              HttpEntity(ContentTypes.`application/json`, Json.stringify(json))
            }
          }
        }
      }
    }

  def routeRemoveMute(implicit ec: ExecutionContext): Route =
    path("api" / "removeMute") {
      post {
        entity(as[MuteDto]) { muteDto =>
          complete {
            removeMuteCrl(muteDto.from, muteDto.to) map { json =>
              HttpEntity(ContentTypes.`application/json`, Json.stringify(json))
            }
          }
        }
      }
    }

  def routeUserLogin(implicit ec: ExecutionContext): Route =
    path("api" / "loginUser") {
      post {
        entity(as[LoginDto]) { dto =>
          complete {
            loginCtl(dto.login, dto.password) map { json =>
              HttpEntity(ContentTypes.`application/json`, Json.stringify(json))
            }
          }
        }
      }
    }

  def routeUserLogout(implicit ec: ExecutionContext): Route =
    path("api" / "logoutUser") {
      post {
        entity(as[UidDto]) { dto =>
          complete {
            logoutCtl(dto.uid) map { json =>
              HttpEntity(ContentTypes.`application/json`, Json.stringify(json))
            }
          }
        }
      }
    }

  def routeGetUserInfo(implicit ec: ExecutionContext): Route =
    path("api" / "getUserInfo") {
      post {
        entity(as[UidDto]) { dto =>
          complete {
            getUserInfoCtl(dto.uid) map { json =>
              HttpEntity(ContentTypes.`application/json`, Json.stringify(json))
            }
          }
        }
      }
    }

  def routeListSessions(implicit ec: ExecutionContext): Route =
    path("api" / "listSessions") {
      post {
        entity(as[ListSessionDto]) { dto =>
          val isPublic = dto.isPublic match {
            case 1 => true
            case _ => false
          }
          complete {
            listSessionsCtl(dto.uid, isPublic) map { json =>
              HttpEntity(ContentTypes.`application/json`, Json.stringify(json))
            }
          }
        }
      }
    }

  def routeListJoinedSessions(implicit ec: ExecutionContext): Route =
    path("api" / "listJoinedSessions") {
      post {
        entity(as[UidDto]) { dto =>
          complete {
            listJoinedSessionsCtl(dto.uid) map { json =>
              HttpEntity(ContentTypes.`application/json`, Json.stringify(json))
            }
          }
        }
      }
    }

  def routeListMessages(implicit ec: ExecutionContext): Route =
    path("api" / "listMessages") {
      post {
        entity(as[ListMessageDto]) { dto =>
          complete {
            listMessagesCtl(dto.uid, dto.sessionid, dto.page, dto.count) map {
              json =>
                HttpEntity(ContentTypes.`application/json`,
                           Json.stringify(json))
            }
          }
        }
      }
    }

  def routeGetJoinedUsers(implicit ec: ExecutionContext): Route =
    path("api" / "getJoinedUsers") {
      post {
        entity(as[UidSessionidDto]) { dto =>
          complete {
            getJoinedUsersCtl(dto.uid, dto.sessionid) map { json =>
              HttpEntity(ContentTypes.`application/json`, Json.stringify(json))
            }
          }
        }
      }
    }

  def routeGetUserInfoByName(implicit ec: ExecutionContext): Route =
    path("api" / "getUserInfoByName") {
      post {
        entity(as[NickNameDto]) { dto =>
          complete {
            getUserInfoByNameCtl(dto.nickName) map { json =>
              HttpEntity(ContentTypes.`application/json`, Json.stringify(json))
            }
          }
        }
      }
    }

  def routeGetPrivateSession(implicit ec: ExecutionContext): Route =
    path("api" / "getPrivateSession") {
      post {
        entity(as[UidOuidDto]) { dto =>
          complete {
            getPrivateSessionCtl(dto.uid, dto.ouid) map { json =>
              HttpEntity(ContentTypes.`application/json`, Json.stringify(json))
            }
          }
        }
      }
    }

  def routeGetSessionHeader(implicit ec: ExecutionContext): Route =
    path("api" / "getSessionHeader") {
      post {
        entity(as[UidSessionidDto]) { dto =>
          complete {
            getSessionHeaderCtl(dto.uid, dto.sessionid) map { json =>
              HttpEntity(ContentTypes.`application/json`, Json.stringify(json))
            }
          }
        }
      }
    }

  def routeGetSessionMenu(implicit ec: ExecutionContext): Route =
    path("api" / "getSessionMenu") {
      post {
        entity(as[UidSessionidDto]) { dto =>
          complete {
            getSessionMenuCtl(dto.uid, dto.sessionid) map { json =>
              HttpEntity(ContentTypes.`application/json`, Json.stringify(json))
            }
          }
        }
      }
    }

  def routeGetUserMenu(implicit ec: ExecutionContext): Route =
    path("api" / "getUserMenu") {
      post {
        entity(as[UidDto]) { dto =>
          complete {
            getUserMenuCtl(dto.uid) map { json =>
              HttpEntity(ContentTypes.`application/json`, Json.stringify(json))
            }
          }
        }
      }
    }

}
