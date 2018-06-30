lazy val root = (project in file("."))
	.settings(
		name := "kafka-web",
		scalaVersion := "2.12.0",
		libraryDependencies += "com.typesafe.akka" %% "akka-http" % "10.1.3",
		libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.13",
		libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.5.13"
	)
