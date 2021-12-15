package Operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_Oper_Transform {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    conf.set("spark.local.dir","d:/test")
    val sc = new SparkContext(conf)

    //todo 算子--转换--
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    val rdd1: RDD[Int] = sc.makeRDD(List(3,4,5,6),2)

    //交集
    println(rdd.intersection(rdd1).collect().mkString(","))
    //并集
    println(rdd.union(rdd1).collect().mkString(","))

    //差集
    println(rdd.subtract(rdd1).collect().mkString(","))

    //拉链 条件：分区里的数据个数相等，同时分区数也要相等
    println(rdd.zip(rdd1).collect().mkString(","))




    sc.stop()
  }

}
