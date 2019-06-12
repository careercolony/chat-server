package com.mj.chat.model

case class MuteDto(from: String, to: String)

case class LoginDto(login: String, password: String)

case class UidDto(uid: String)

case class NickNameDto(nickName: String)

case class UidSessionidDto(uid: String, sessionid: String)

case class UidOuidDto(uid: String, ouid: String)

case class ListSessionDto(uid: String, isPublic: Int)

case class RegisterDto(login: String,
                       nickname: String,
                       password: String,
                       repassword: String,
                       gender: Int)

case class ListMessageDto(uid: String, sessionid: String, page: Int, count: Int)
