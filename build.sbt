name := "EnvironmentHealth"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.2.0"
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0-RC2"
libraryDependencies += "io.spray" %%  "spray-json" % "1.3.5"
libraryDependencies += "org.mongodb.scala" %% "mongo-scala-bson" % "4.1.0"
libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.2"
libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.9.0"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
