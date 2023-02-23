course := "concpar"
assignment := "pubsub"

scalaVersion := "3.1.1"

scalacOptions ++= Seq("-language:implicitConversions", "-deprecation")
libraryDependencies += "org.scalameta" %% "munit" % "0.7.26" % Test
