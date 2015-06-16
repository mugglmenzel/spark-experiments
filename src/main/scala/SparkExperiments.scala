import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by mugglmenzel on 15/06/15.
 */
object SparkExperiments extends App {

  val sc = new SparkContext(new SparkConf().setAppName("spark-experiment").setMaster("local[4]"))

  sc.parallelize(Seq("abc", "abcdef", "qwertz", "i", "auffahrtsturm"), 4)
    .map(i => (i, i.length)).collect().toList
    .sortBy(_._2)(Ordering[Int].reverse)
    .foreach(println)


  lazy val darwin = sc.textFile(SparkExperiments.getClass.getResource("pg2009.txt").toString)
  lazy val inputFolder = sc.wholeTextFiles(SparkExperiments.getClass.getResource("input").toString)

  printHotWords(darwin, 10)

  printHotWords(inputFolder.map(_._2), 5)




  def printHotWords(documents: RDD[String], maxListSize: Int) =
    documents.flatMap(_.split(" ")).cache().map(word => (word, word.length))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(maxListSize).foreach(println)
}
