package com.mj.chat.websocket

import akka.actor.ActorRef
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.mj.chat.model._
import com.mj.chat.mongo.MongoLogic.{isMute, _}
import com.mj.chat.tools.CommonUtils._

class ChatSessionActor extends TraitPubSubActor {
  val system = context.system

  import system.dispatcher

  import DistributedPubSubMediator._

  val mediator = DistributedPubSub(context.system).mediator

  //actorRef is stream's actorRef
  var actorRef = ActorRef.noSender

  //chat session actor related info
  var uid = ""
  var nickname = ""
  var avatar = ""
  var sessionid = ""
  var sessionName = ""
  var sessionIcon = ""

  def receive: Receive = eventReceive orElse {
    case SubscribeAck(Subscribe(ssessionid, None, `self`)) if sessionid != "" =>
      //publish user join session
      mediator ! Publish(sessionid,
                         ClusterText(uid,
                                     nickname,
                                     avatar,
                                     sessionid,
                                     sessionName,
                                     sessionIcon,
                                     "online",
                                     s"User $nickname online session"))
      updateUserOnlineOffline(uid, sessionid, isOnline = true)
      consoleLog("SUCCESSFUL", s"User $nickname online session $sessionid")

    case UnsubscribeAck(Unsubscribe(ssessionid, None, `self`)) =>
      //publish user left session
      actorRef = ActorRef.noSender
      mediator ! Publish(sessionid,
                         ClusterText(uid,
                                     nickname,
                                     avatar,
                                     sessionid,
                                     sessionName,
                                     sessionIcon,
                                     "offline",
                                     s"User $nickname offline session"))
      updateUserOnlineOffline(uid, sessionid, isOnline = false)
      consoleLog("SUCCESSFUL", s"User $nickname offline session $sessionid")

    case UserOnline(ref) =>
      //when websocket stream create it will send UserOnline to akka cluster
      //update the actorRef to websocket stream actor reference
      actorRef = ref

    case UserOffline if sessionid != "" =>
      //when websocket stream close it will send UserOffline to akka cluster
      //unsubscribe current session
      mediator ! Unsubscribe(sessionid, self)

    case WsTextDown(suid,
                    snickname,
                    savatar,
                    ssessionid,
                    ssessionName,
                    ssessionIcon,
                    msgType,
                    content,
                    dateline) if ssessionid != "" =>
      if (msgType == "online") {
        //user online a session
        uid = suid
        nickname = snickname
        avatar = savatar
        sessionid = ssessionid
        sessionName = ssessionName
        sessionIcon = ssessionIcon
        mediator ! Subscribe(sessionid, self)
      } else {
        //user send text message
        uid = suid
        nickname = snickname
        avatar = savatar
        sessionid = ssessionid
        sessionName = ssessionName
        sessionIcon = ssessionIcon

        for {
          ismute <- isMute(suid, sessionid)
        } yield {
          if (ismute) {
            actorRef ! WsTextDown(suid,
                                  snickname,
                                  savatar,
                                  sessionid,
                                  sessionName,
                                  sessionIcon,
                                  "mute",
                                  "",
                                  dateline)
          } else {
            mediator ! Publish(sessionid,
                               ClusterText(uid,
                                           nickname,
                                           avatar,
                                           sessionid,
                                           sessionName,
                                           sessionIcon,
                                           msgType,
                                           content,
                                           dateline))
            insertMessage(uid, sessionid, msgType, content = content)
          }
        }

      }

    case ClusterText(suid,
                     snickname,
                     savatar,
                     ssessionid,
                     ssessionName,
                     ssessionIcon,
                     msgType,
                     content,
                     dateline) if actorRef != ActorRef.noSender =>
      //when receive cluster push message
      //send back to websocket stream
      getSessionNameIcon(suid, ssessionid).map { sessionToken =>
        actorRef ! WsTextDown(suid,
                              snickname,
                              savatar,
                              sessionToken.sessionid,
                              sessionToken.sessionName,
                              sessionToken.sessionIcon,
                              msgType,
                              content,
                              dateline)
      }

  }
}
