//import Dependencies._
name := "SPARK_POC"
version := "0.1"
//organization := "MyMegaCorp"
scalaVersion := "2.11.12"
//scalaVersion := "2.11.8"
val sparkVersion = "2.4.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
// "org.apache.spark" %% "spark-streaming-kafka_2.11" % sparkVersion,
  "mysql" % "mysql-connector-java" % "5.1.6",
//  "com.databricks"  % "spark-xml" % "0.5.0",
//  "com.databricks"  % "spark-avro_2.11" % "3.2.0" ,
  "org.apache.kafka" % "kafka_2.11" % "2.1.1" ,
  "org.apache.kafka" % "kafka-clients" % "1.0.0",
 // "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"
)

//resolvers += Classpaths.typesafeResolvercd 
//addSbtPlugin("com.typesafe.startscript" % "xsbt-start-script-plugin" % "0.5.3")
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.8"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.8"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.8"


updateOptions := updateOptions.value.withCachedResolution(true)
libraryDependencies += "com.typesafe" % "config" % "1.3.0"
