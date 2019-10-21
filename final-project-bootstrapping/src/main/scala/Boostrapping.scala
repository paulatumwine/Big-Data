import org.apache.spark.sql.functions.{avg, variance}
import org.apache.spark.{SparkConf, SparkContext}

case class Arrests(id: String, released: String, colour: String, year: String, age: String, sex: String,
                   employed: String, citizen: String, checks: String)

object Boostrapping extends App {

    override def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("Bootstrapping").setMaster("local")
        val sc = new SparkContext(conf)
        sc.setLogLevel("WARN")
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext.implicits._

        // Load the data file
        val csv = sc.textFile("input/Arrests.csv")

        // Create a Spark DataFrame
        val headerAndRows = csv.map(line => line.split(",").map(_.trim))
        val header = headerAndRows.first
        val data = headerAndRows.filter(_ (0) != header(0))
        val population = data
            .map(p => Arrests(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8)))
            .toDF
        // output the schema for debugging purposes
        // population.printSchema
        
        // output the average and variance of the overall population
        population.groupBy("sex").agg(avg("age").as("average")).show()
        population.groupBy("sex").agg(variance("age").as("variance")).show()

        // take initial sample from whole population with no replacement
        val sample = population.sample(false, .25)
        // sample.show(10) // debug

        // compute average and variance metrics for sample taken
        val average = sample
            .groupBy("sex")
            .agg(avg("age").as("average"))
            .flatMap(row => Map(row.getString(0) -> row.getDouble(1)))
            .collect()
            .toList
        val variace = sample
            .groupBy("sex")
            .agg(variance("age").as("variance"))
            .flatMap(row => Map(row.getString(0) -> row.getDouble(1)))
            .collect()
            .toList

        // average.foreach(println(_)) // debug
        // variace.foreach(println(_)) // debug

        // add average and variance of initial sample to the collections of the corresponding metrics
        var averages: List[(String, Double)] = average
        var variances: List[(String, Double)] = variace

        // resample with replacement, compute average & variance metrics for each sample, and append them to corresponding collections
        val n = 10
        for (index <- 1 to n - 1) {
            val resample = sample.sample(true, 1)
            averages :::= resample.groupBy("sex").agg(avg("age").as("average"))
                .flatMap(row => Map(row.getString(0) -> row.getDouble(1)))
                .collect()
                .toList
            variances :::= resample.groupBy("sex").agg(variance("age").as("variance"))
                .flatMap(row => Map(row.getString(0) -> row.getDouble(1)))
                .collect()
                .toList
        }

        // reduce collections to get average after n samples
        val bootAvg = sc.parallelize(averages).reduceByKey(_+_).map(x => x._1 -> x._2 / n).toDF()
        val bootVar = sc.parallelize(variances).reduceByKey(_+_).map(x => x._1 -> x._2 / n).toDF()
        bootAvg.show()
        bootVar.show()
    }
}
