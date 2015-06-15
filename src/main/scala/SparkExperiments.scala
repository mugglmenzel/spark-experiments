import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by mugglmenzel on 15/06/15.
 */
object SparkExperiments extends App {

  val sc = new SparkContext(new SparkConf().setAppName("spark-experiment").setMaster("local[4]"))

  sc.parallelize(Seq("abc", "abcdef", "qwertz", "i", "auffahrtsturm"), 4)
    .map(i => (i, i.length)).collect().toList
    .sortBy(_._2)(Ordering[Int].reverse)
    .foreach(println)

}
