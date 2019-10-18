package main.scala.samples.starters

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * http://spark.apache.org/docs/latest/quick-start.html
  */
object SimpleApp {
    def main(args: Array[String]) {
//        val logFile = "YOUR_SPARK_HOME/README.md" // Should be some file on your system
        val logFile = "/usr/local/share/spark/README.md" // Should be some file on your system
        val conf = new SparkConf()
        conf.setMaster("local")
        val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()
        val logData = spark.read.textFile(logFile).cache()
        val numAs = logData.filter(line => line.contains("a")).count()
        val numBs = logData.filter(line => line.contains("b")).count()
        println(s"Lines with a: $numAs, Lines with b: $numBs")
        spark.stop()
    }
}
