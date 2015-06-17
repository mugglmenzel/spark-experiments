import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by mugglmenzel on 15/06/15.
 */
object SparkExperiments extends App {

  val sc = new SparkContext(new SparkConf().setAppName("spark-experiment").setMaster("local[4]"))

  lazy val fooSeq = Seq("abc", "abcdef", "qwertz", "i", "auffahrtsturm")

  println("Computing a set sorted by length from input " + fooSeq + ":\n----------\n\n")

  sc.parallelize(fooSeq, 4)
    .map(i => (i, i.length)).collect().toList
    .sortBy(_._2)(Ordering[Int].reverse)
    .foreach(println)
  println("\n\n")

  lazy val darwin = sc.textFile(SparkExperiments.getClass.getResource("pg2009.txt").toString)
  lazy val inputFolder = sc.wholeTextFiles(SparkExperiments.getClass.getResource("input").toString)
  lazy val wordsInInputFolder = convertToWords(inputFolder.map(_._2))

  println("Computing top 10 sorted word count for Darwin's 'Origin of Species':\n----------\n\n")
  printHotWords(convertToWords(darwin), 10)
  println("\n\n")
  println("Computing top 5 sorted word count over all files in folder 'input':\n----------\n\n")
  printHotWords(wordsInInputFolder, 5)
  println("\n\n")
  inputFolder.foreach{case (fileName: String, content: String) =>
    println("Computing top 5 sorted word count for " + fileName.reverse.takeWhile(_.toString.ne("/")).reverse + ":\n----------\n\n")
    printHotWords(convertToWords(sc.parallelize(Seq(content))), 5)
    println("\n\n")
  }

  println("Computing top 5 sorted word count for words starting with 'm' over all files in folder 'input':\n----------\n\n")
  printHotWords(wordsInInputFolder.filter(_.startsWith("m")), 5)


  def convertToWords(documents: RDD[String]) = documents.flatMap(_.split(" ")).cache()

  def printHotWords(documents: RDD[String], maxListSize: Int) =
    documents.map(word => (word, word.length))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(maxListSize).foreach(println)
}
