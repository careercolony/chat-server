package com.mj.chat.config

import com.typesafe.config.{Config, ConfigFactory}

object Settings {
  val config: Config = ConfigFactory.load("application.conf")

  val configServer: Config = config.getConfig("server")
  val hostName: String = configServer.getString("hostName")
  val port: Int = configServer.getString("port").toInt
  val akkaPort: Int = configServer.getString("akkaPort").toInt
  val seedNodes: String = configServer.getString("seedNodes")

  val configMongo: Config = config.getConfig("mongodb")
  val configMongoDbname: String = configMongo.getString("dbname")
  var configMongoUri: String = configMongo.getString("uri")

}
