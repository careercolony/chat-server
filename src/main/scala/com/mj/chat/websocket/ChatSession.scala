package com.mj.chat.websocket

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.model.ws.TextMessage.Strict
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.stream._
import akka.stream.scaladsl._
import com.mj.chat.model._
import com.mj.chat.tools.CommonUtils._
import play.api.libs.json.Json

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class ChatSession()(implicit ec: ExecutionContext, actorSystem: ActorSystem, materializer: ActorMaterializer) {
  implicit val chatMessageWrites = Json.writes[ChatMessage]

  val chatSessionActor: ActorRef = actorSystem.actorOf(Props(classOf[ChatSessionActor]))
  consoleLog("INFO", s"create new chatSessionActor: $chatSessionActor")

  val source: Source[WsMessageDown, ActorRef] = Source.actorRef[WsMessageDown](bufferSize = Int.MaxValue, OverflowStrategy.fail)

  def chatService: Flow[Message, Strict, ActorRef] = Flow.fromGraph(GraphDSL.create(source) {
    implicit builder =>
      (chatSource: Source[WsMessageDown, ActorRef]#Shape) =>
        import GraphDSL.Implicits._

        val flowFromWs: FlowShape[Message, WsMessageUp] = builder.add(
          Flow[Message].collect {
            case tm: TextMessage =>
              tm.textStream.runFold("")(_ + _).map { jsonStr =>
                Try {
                  val json = Json.parse(jsonStr)
                  val uid: String = getJsonString(json, "uid")
                  val nickname: String = getJsonString(json, "nickname")
                  val avatar: String = getJsonString(json, "avatar")
                  val sessionid: String = getJsonString(json, "sessionid")
                  val sessionName: String = getJsonString(json, "sessionName")
                  val sessionIcon: String = getJsonString(json, "sessionIcon")
                  val msgType: String = getJsonString(json, "msgType")
                  val content: String = getJsonString(json, "content")
                  WsTextUp(uid, nickname, avatar, sessionid, sessionName, sessionIcon, msgType, content)
                }.recover {
                  case e: Throwable =>
                    consoleLog("ERROR", s"parse websocket text message error: $e")
                    throw e
                }.getOrElse(
                  WsTextUp("", "", "", "", "", "", "", "")
                )
              }
          }.buffer(1024 * 1024, OverflowStrategy.fail).mapAsync(6)(t => t)
        )

        val broadcastWs: UniformFanOutShape[WsMessageUp, WsMessageUp] = builder.add(Broadcast[WsMessageUp](2))

        val filterFailure: FlowShape[WsMessageUp, WsMessageUp] = builder.add(Flow[WsMessageUp].filter(_.uid == ""))
        val flowReject: FlowShape[WsMessageUp, WsTextDown] = builder.add(
          Flow[WsMessageUp].map(_ => WsTextDown("", "", "", "", "", "", "reject", "no privilege to send message"))
        )

        val filterSuccess: FlowShape[WsMessageUp, WsMessageUp] = builder.add(Flow[WsMessageUp].filter(_.uid != ""))

        val flowAccept: FlowShape[WsMessageUp, WsMessageDown] = builder.add(
          Flow[WsMessageUp].collect {
            case WsTextUp(uid, nickname, avatar, sessionid, sessionName, sessionIcon, msgType, content) =>
              Future(
                WsTextDown(uid, nickname, avatar, sessionid, sessionName, sessionIcon, msgType, content)
              )
          }.buffer(1024 * 1024, OverflowStrategy.fail).mapAsync(6)(t => t)
        )

        val mergeAccept: UniformFanInShape[WsMessageDown, WsMessageDown] = builder.add(Merge[WsMessageDown](2))

        val connectedWs: Flow[ActorRef, UserOnline, NotUsed] = Flow[ActorRef].map { actor =>
          UserOnline(actor)
        }

        val chatActorSink: Sink[WsMessageDown, NotUsed] = Sink.actorRef[WsMessageDown](chatSessionActor, UserOffline)

        //0: accepted, 1: rejected
        val flowAcceptBack: FlowShape[WsMessageDown, WsMessageDown] = builder.add(
          // websocket default timeout after 60 second, to prevent timeout send keepalive message
          // you can config akka.http.server.idle-ti
          // meout to set timeout duration
          Flow[WsMessageDown].keepAlive(50.seconds, () => WsTextDown("", "", "", "", "", "", "keepalive", ""))
        )

        val mergeBackWs: UniformFanInShape[WsMessageDown, WsMessageDown] = builder.add(Merge[WsMessageDown](2))

        val flowBackWs: FlowShape[WsMessageDown, Strict] = builder.add(
          Flow[WsMessageDown].collect {
            case WsTextDown(uid, nickname, avatar, sessionid, sessionName, sessionIcon, msgType, content, dateline) =>
              val chatMessage = ChatMessage(uid, nickname, avatar, msgType, content, fileName = "", fileType = "", fileid = "", thumbid = "", dateline)
              TextMessage(Json.stringify(Json.toJson(chatMessage)))
          }
        )
        flowFromWs ~> broadcastWs
        broadcastWs ~> filterFailure ~> flowReject
        broadcastWs ~> filterSuccess ~> flowAccept ~> mergeAccept.in(0)
        builder.materializedValue ~> connectedWs ~> mergeAccept.in(1)

        mergeAccept ~> chatActorSink // --> to chatSessionActor

        /* from chatSessionActor --> */
        chatSource ~> flowAcceptBack ~> mergeBackWs.in(0)
        flowReject ~> mergeBackWs.in(1)

        mergeBackWs ~> flowBackWs

        FlowShape(flowFromWs.in, flowBackWs.out)
  })

}