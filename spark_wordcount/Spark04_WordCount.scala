package spark_wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark04_WordCount {
  def main(args: Array[String]): Unit = {
      //todo 使用spark
      val conf = new SparkConf().setMaster("local").setAppName("WordCount")
      val sc = new SparkContext(conf)

    //读取文件
    var lines: RDD[String] = sc.textFile("data/word.txt")

    //将文件中的数据进行了分词
    //word=>(word,1)
    val words = lines.flatMap(_.split(" "))
    val wordsToOne = words.map((_, 1))

    //reduceByKey ：按照key分组，对相同key的value进行聚合
    //框架的核心就是封装
    val wordCount = wordsToOne.reduceByKey(_+_)

    //将结果打印在控制台
    wordCount.collect().foreach(println)

    sc.stop()
  }

}
