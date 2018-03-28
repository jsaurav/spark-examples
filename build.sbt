name := "spark-examples"
version := "1.0"
scalaVersion := "2.11.0"

val sparkVersion = "2.2.0" 
mainClass in (Compile, run) := Some("com.techidiocy.spark.examples.WordCount")


libraryDependencies += "org.apache.spark" % "spark-core_2.11" % sparkVersion
