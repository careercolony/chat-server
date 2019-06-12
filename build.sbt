name := "Chat"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.8"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= {
  val akkaV = "2.5.16"
  val akkaHttpV = "10.1.5"
  Seq(
    "com.typesafe.akka" %% "akka-actor"  % akkaV,
    "com.typesafe.akka" %% "akka-cluster" % akkaV,
    "com.typesafe.akka" %% "akka-cluster-tools" % akkaV,
    "com.typesafe.akka" %% "akka-testkit" % akkaV % Test,
    "com.typesafe.akka" %% "akka-stream" % akkaV,
    "com.typesafe.akka" %% "akka-stream-testkit" % akkaV % Test,
    "com.typesafe.akka" %% "akka-http" % akkaHttpV,
    "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpV % Test,
    "com.typesafe.akka" %% "akka-http-spray-json-experimental" % "2.4.11.2",
    "com.typesafe.play" %% "play-json" % "2.5.15",
    "org.slf4j" % "slf4j-simple" % "1.7.25",
    "com.sksamuel.scrimage" %% "scrimage-core" % "2.1.8",
    "com.sksamuel.scrimage" %% "scrimage-io-extra" % "2.1.8",
    "com.esotericsoftware" % "kryo" % "4.0.0",
    "com.github.romix.akka" %% "akka-kryo-serialization" % "0.5.0",
    "commons-cli" % "commons-cli" % "1.4",
    "io.jsonwebtoken" % "jjwt" % "0.7.0",
    "org.reactivemongo" %% "reactivemongo" %  "0.12.7",
    "org.reactivemongo" %% "reactivemongo-play-json" %  "0.12.7-play26"
 )
}
