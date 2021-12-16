package Persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


object Spark04_WordCount$Spark04_WordCount {
  def main(args: Array[String]): Unit = {
      //todo 使用spark
      val conf = new SparkConf().setMaster("local").setAppName("WordCount")
      val sc = new SparkContext(conf)

    //读取文件
    var lines: RDD[String] = sc.textFile("data/word.txt")
    val words = lines.flatMap(_.split(" "))
    val wordsToOne = words.map((_, 1))
    val wordCount = wordsToOne.reduceByKey(_+_)
    wordCount.collect().foreach(println)

    //数据的持久化
    // cache方法可以将血缘关系进行修改，添加了一个和缓存相关的依赖关系
    //cache操作不安全。
    wordsToOne.cache()
    wordsToOne.persist(StorageLevel.DISK_ONLY_2)
    //持久化的文件只能自己用，而且使用完毕后会删除


    // 如果没有数据的持久化，执行过的算子，不会保留数据，需要从最开始的步骤重新执行一遍
//    var lines1: RDD[String] = sc.textFile("data/word.txt")
//
//    val words1 = lines1.flatMap(_.split(" "))
//    val wordsToOne1 = words1.map((_, 1))
    print("---------------------------------------")
    val WordCount1: RDD[(String, Iterable[(String, Int)])] = wordsToOne.groupBy(_._1)
    WordCount1.collect().foreach(println)


    sc.stop()
  }

}
