package main.scala.samples.starters

import org.apache.spark.{SparkConf, SparkContext}

/**
  * https://www.cloudera.com/tutorials/setting-up-a-spark-development-environment-with-scala/.html
  */
object SampleApp {

    def main(args: Array[String]) {

        //Create a SparkContext to initialize Spark
        val conf = new SparkConf()
        conf.setMaster("local")
        conf.setAppName("Word Count")
        val sc = new SparkContext(conf)

        // Load the text into a Spark RDD, which is a distributed representation of each line of text
        val textFile = sc.textFile("src/main/resources/shakespeare.txt")

        //word count
        val counts = textFile.flatMap(line => line.split(" "))
            .map(word => (word, 1))
            .reduceByKey(_ + _)

        counts.foreach(println)
        System.out.println("Total words: " + counts.count());
        counts.saveAsTextFile("/tmp/shakespeareWordCount")
    }

}
