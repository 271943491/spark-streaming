name := "SparkKafka"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.0"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.0.0"

libraryDependencies += "org.apache.hbase" % "hbase" % "1.0.0"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.0.0"


assemblyMergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
