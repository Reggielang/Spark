package IO

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark04_IO {
  def main(args: Array[String]): Unit = {
      //todo 使用spark
      val conf = new SparkConf().setMaster("local").setAppName("WordCount")
      val sc = new SparkContext(conf)

    //读取文件
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2)), 2)

    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output1")
    rdd.saveAsSequenceFile("output2")



    val rdd1: RDD[(String,Int)] = sc.objectFile("output1")

    rdd1.collect().foreach(println)

    val rdd2: RDD[(String,Int)] = sc.sequenceFile("output2")
    rdd2.collect().foreach(println)




    sc.stop()
  }

}
