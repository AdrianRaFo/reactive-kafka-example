scalacOptions += "-Ywarn-unused-import"

val kafkaVersion = "0.11.0.0"

scalaVersion := "2.12.3"

val kafkaDeps = Seq(
  "org.apache.kafka" %% "kafka"        % kafkaVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion
)

val afkaDeps = Seq(
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.17",
  "com.typesafe.akka" %% "akka-stream" % "2.4.18",
  "com.typesafe.akka" %% "akka-actor" % "2.4.18",
  "com.typesafe.akka" % "akka-slf4j_2.12" % "2.4.18",
  "ch.qos.logback" % "logback-classic" % "1.2.3"

)

lazy val kafka = project in file(".") aggregate (kclient, afka)

lazy val kclient = project in file("kclient") settings (libraryDependencies ++= kafkaDeps)

lazy val afka = project in file("afka") settings (libraryDependencies ++= afkaDeps)

fork in run := true
