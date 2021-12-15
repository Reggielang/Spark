package Operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Oper_Transform {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    conf.set("spark.local.dir","d:/test")
    val sc = new SparkContext(conf)

    //todo 算子--转换-- coalesce 缩减分区
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),3)

    //缩减(合并)，默认情况下，缩减分区不会进行shuffle
    //这种方式在某些情况下，不能解决数据倾斜问题，所以还可以在缩减分区的同时，进行shuffle操作
    val rdd1: RDD[Int] = rdd.coalesce(2)

    val rdd2: RDD[Int] = rdd.coalesce(2,true)


    rdd.saveAsTextFile("output")
    rdd1.saveAsTextFile("output1")
    rdd2.saveAsTextFile("output2")

    sc.stop()
  }

}
