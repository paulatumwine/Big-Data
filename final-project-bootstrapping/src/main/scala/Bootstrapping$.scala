import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, variance}
import org.apache.spark.{SparkConf, SparkContext}

case class Arrests(id: String, released: String, colour: String, year: String, age: String, sex: String,
                   employed: String, citizen: String, checks: String)

object Bootstrapping$ extends App {

    override def main(args: Array[String]): Unit = {

        // Retrieve args if any
        val category = if (args.length > 0 && args(0) != null) args(0) else "sex"; // Category Variable
        val numeric = if (args.length > 0 && args(1) != null) args(1) else "age"; // Numeric Variable
        val samples: Int = if (args.length > 0 && args(2) != null) args(2).asInstanceOf[Int] else 10; // The number of samples to work with

        val conf = new SparkConf().setAppName("Bootstrapping").setMaster("local")
        val sc = new SparkContext(conf)
        sc.setLogLevel("WARN")
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext.implicits._

        // Load the data file (from https://vincentarelbundock.github.io/Rdatasets/doc/carData/Arrests.html)
        val csv = sc.textFile("input/Arrests.csv")

        // Create a Spark DataFrame
        val headerAndRows = csv.map(line => line.split(",").map(_.trim))
        val header = headerAndRows.first
        val data = headerAndRows.filter(_ (0) != header(0))
        val population = data
            .map(p => Arrests(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8)))
            .toDF

        // output the average and variance of the overall population
        val popnAvg = population.groupBy(category).agg(avg(numeric).as("average"))
        val popnVar = population.groupBy(category).agg(variance(numeric).as("variance"))

        // take initial sample from whole population with no replacement
        val sample = population.sample(false, .25)

        // compute average and variance metrics for sample taken
        val average = sample.groupBy(category).agg(avg(numeric).as("average")).toDF
        val variace = sample.groupBy(category).agg(variance(numeric).as("variance")).toDF

        // add average and variance of initial sample to the collections of the corresponding metrics
        var avgColl: List[DataFrame] = List(average)
        var varColl: List[DataFrame] = List(variace)

        // resample with replacement, compute average & variance metrics for each sample, and append them to corresponding collections
        for (i <- 1 until (samples - 1)) {
            val resample = sample.sample(true, 1)
            avgColl ::= resample.groupBy(category).agg(avg(numeric)).toDF
            varColl ::= resample.groupBy(category).agg(variance(numeric)).toDF
        }

        // reduce respective collections to get average & variance across n samples
        val flatAvg = avgColl
            .flatMap(df => df.flatMap(row => Map(row.getString(0) -> row.getDouble(1))).collect().toList)
        val sampAvg = sc.parallelize(flatAvg)
            .reduceByKey(_ + _)
            .map(x => x._1 -> x._2 / samples)
            .toDF

        val flatVar = varColl
            .flatMap(df => df.map(row => row.getString(0) -> row.getDouble(1)).collect().toList)
        val sampVar = sc.parallelize(flatVar)
            .reduceByKey(_ + _)
            .map(x => x._1 -> x._2 / samples)
            .toDF()

        // Output away...
        popnAvg.show()
        sampAvg.show()

        popnVar.show()
        sampVar.show()
    }
}
