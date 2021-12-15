package Operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark15_RDD_Oper_Transform {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    conf.set("spark.local.dir","d:/test")
    val sc = new SparkContext(conf)

    //todo 算子--转换--ReduceByKey
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 1), ("a", 1)))

    //ReduceByKey 是将相同的key的value分在一个组，然后进行reduce操作
    //todo ReduceByKey 可以实现word count
    val wordCount: RDD[(String, Int)] = rdd.reduceByKey(_ + _)

    wordCount.collect().foreach(print)


    sc.stop()
  }

}
