import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.reflect.ClassTag
import scala.reflect.io.Path
import scala.util.Try

/**
 * Created by mugglmenzel on 15/06/15.
 */
object SparkExperiments extends App {

  /*
   * Spark context
   */

  val sc = new SparkContext(new SparkConf().setAppName("spark-experiment").setMaster("local[4]"))

  /*
   * Input data
   */
  lazy val fooSeq = Seq("abc", "abcdef", "qwertz", "i", "auffahrtsturm")
  lazy val darwin = sc.textFile(SparkExperiments.getClass.getResource("pg2009.txt").toString)
  lazy val inputFolder = sc.wholeTextFiles(SparkExperiments.getClass.getResource("input").toString)
  lazy val wordsInInputFolder = convertToWords(inputFolder.map(_._2))


  /*
   * Simple experiments
   */

  println("Computing a set sorted by length from input " + fooSeq + ":\n----------\n\n")

  sc.parallelize(fooSeq, 4)
    .map(i => (i, i.length)).collect().toList
    .sortBy(_._2)(Ordering[Int].reverse)
    .foreach(println)
  println("\n\n")



  /*
   * Word count experiments
   */

  println("Computing top 10 sorted word count for Darwin's 'Origin of Species':\n----------\n\n")
  printHotWords(sortWordCounts(countWords(convertToWords(darwin))), 10)
  println("\n\n")
  println("Computing top 5 sorted word count over all files in folder 'input':\n----------\n\n")
  printHotWords(countWords(wordsInInputFolder).collect(), 5)
  println("\n\n")
  inputFolder.foreach { case (fileName: String, content: String) =>
    val actFileName = fileName.reverse.takeWhile(_ != '/').reverse
    Try(Option(Path("hot_5_word_count_" + actFileName)).filter(_.exists).map(_.deleteRecursively())).foreach { clean =>
      println("Computing top 5 sorted word count for " + actFileName + ":\n----------\n\n")
      printHotWords(sortWordCounts(countWords(convertToWords(sc.parallelize(Seq(content))))), 5).saveAsTextFile("hot_5_word_count_" + actFileName)
      println("\n\n")
    }
  }

  Try(Option(Path("hot_5_word_count_with_m")).filter(_.exists).map(_.deleteRecursively())).foreach { clean =>
    println("Computing top 5 sorted word count for words starting with 'm' over all files in folder 'input':\n----------\n\n")
    printHotWords(sortWordCounts(countWords(wordsInInputFolder.filter(_.startsWith("m")))), 5).saveAsTextFile("hot_5_word_count_with_m")
  }

  def convertToWords(documents: RDD[String]) = documents.flatMap(_.split(" ")).cache()

  def countWords(documents: RDD[String]) =
    documents.map(word => (word, word.length))
      .reduceByKey(_ + _).cache()

  def sortWordCounts(documents: RDD[(String, Int)]) =
    documents.collect().sortBy(_._2)(Ordering[Int].reverse)

  def printHotWords(documents: Array[(String, Int)], maxListSize: Int) = {
    documents.take(maxListSize).foreach(println)
    sc.parallelize(documents)
  }

  /*
   * Split experiments
   */

  println("Splitting RDDs from folder 'input' by occurence of letter 'p' in file name:\n----------\n\n")
  val (rdd1, rdd2) = splitRDD(inputFolder){e =>
    e._1.reverse.takeWhile(_ != '/').reverse.contains('p')
  }
  println("RDD1: #" + rdd1.count() + " vs. RDD2: #" + rdd2.count() + "\n\n")
  println("Files in split RDD1:\n")
  rdd1.foreach(e => println(e._1))
  println("\n\n")
  println("Files in split RDD2:\n")
  rdd2.foreach(e => println(e._1))

  def copyRDD[A: ClassTag](rdd: RDD[A]) = {
    val cachedRdd = rdd.cache()
    (cachedRdd, sc.parallelize[A](Seq.empty) ++ cachedRdd)
  }

  def splitRDD[A](rdd: RDD[A])(splitter: (A) => Boolean) = {
    val cachedRdd = rdd.cache()
    (cachedRdd.filter(splitter), cachedRdd.filter(splitter.andThen(!_)))
  }



  /*
   * Join experiments
   */

  //rdd1.join(rdd2).foreach(e => println(e))



}
