package Dep

import org.apache.spark.{SparkConf, SparkContext}


object Spark01_WordCount_dep02 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("data/word.txt")
    println(lines.dependencies)
    println("**********************")
    val words = lines.flatMap(_.split(" "))
    println(words.dependencies)
    println("**********************")

    val wordGroup = words.groupBy(word => word)
    println(wordGroup.dependencies)
    println("**********************")

    val wordCount = wordGroup.mapValues(_.size)
    println(wordCount.dependencies)
    println("**********************")


    //依赖关系主要分为两大类

    //窄依赖 OneToOneDependency
    //宽依赖 ShuffleDependency
    wordCount.collect().foreach(println)

    sc.stop()
  }

}
