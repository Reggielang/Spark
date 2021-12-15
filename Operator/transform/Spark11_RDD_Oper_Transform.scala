package Operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Oper_Transform {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    conf.set("spark.local.dir","d:/test")
    val sc = new SparkContext(conf)

    //todo 算子--转换-- repartition

    //扩大分区
    //在不shuffle的情况下，扩大分区是没有意义的
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),2)

    val rdd1: RDD[Int] = rdd.coalesce(3,true)

    //重新分区
    val rdd2: RDD[Int] = rdd.repartition(3)

    rdd1.saveAsTextFile("output")

    sc.stop()
  }

}
