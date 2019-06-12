package com.mj.chat.tools

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.mj.chat.model._
import spray.json._

object JsonRepo extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val muteDtoFormats: RootJsonFormat[MuteDto] = jsonFormat2(MuteDto)
  implicit val loginDtoFormats: RootJsonFormat[LoginDto] = jsonFormat2(LoginDto)
  implicit val uidSessionidDtoFormats: RootJsonFormat[UidSessionidDto] =
    jsonFormat2(UidSessionidDto)
  implicit val uidOuidDtoFormats: RootJsonFormat[UidOuidDto] = jsonFormat2(
    UidOuidDto)
  implicit val listSessionDtoFormats: RootJsonFormat[ListSessionDto] =
    jsonFormat2(ListSessionDto)
  implicit val uidDtoFormats: RootJsonFormat[UidDto] = jsonFormat1(UidDto)
  implicit val nickNameDtoFormats: RootJsonFormat[NickNameDto] = jsonFormat1(
    NickNameDto)
  implicit val registerDtoFormats: RootJsonFormat[RegisterDto] = jsonFormat5(
    RegisterDto)
  implicit val listMessageDtoFormats: RootJsonFormat[ListMessageDto] =
    jsonFormat4(ListMessageDto)

}
