package Dep

import org.apache.spark.{SparkConf, SparkContext}


object Spark01_WordCount_dep {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("data/word.txt")
    println(lines.toDebugString)
    println("**********************")
    val words = lines.flatMap(_.split(" "))
    println(words.toDebugString)
    println("**********************")

    val wordGroup = words.groupBy(word => word)
    println(wordGroup.toDebugString)
    println("**********************")

    val wordCount = wordGroup.mapValues(_.size)
    println(wordCount.toDebugString)
    println("**********************")

    wordCount.collect().foreach(println)

    sc.stop()
  }

}
