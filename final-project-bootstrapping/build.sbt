name := "final-project-bootstrapping"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.4.4",
    "org.apache.spark" %% "spark-sql" % "2.4.4",
    "au.com.bytecode" % "opencsv" % "2.4"
)
