lazy val root = (project in file("."))
	.settings(
		name := "kafka-web",
		scalaVersion := "2.12.0",
		libraryDependencies += "com.typesafe.akka" %% "akka-http" % "10.1.3",
		libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.13",
		libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.5.13",
		libraryDependencies += "org.json4s" %% "json4s-native" % "3.5.4",
		libraryDependencies += "com.github.ben-manes.caffeine" % "caffeine" % "2.6.2",
		libraryDependencies += "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.0",
    libraryDependencies += "com.google.code.findbugs" % "jsr305" % "2.0.3", // require by some crap guava used by some crap library
    libraryDependencies += "org.apache.kafka" % "kafka-clients" % "1.1.0"
	)
