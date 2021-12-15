package Operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19_RDD_Oper_Transform {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    conf.set("spark.local.dir","d:/test")
    val sc = new SparkContext(conf)

    //todo 算子--转换--SortByKey
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 2), ("a", 1), ("c", 3), ("b", 4)
    ))
    //SortByKey 按照Key排序
    val rdd1: RDD[(String, Int)] = rdd.sortByKey(false)

    rdd1.collect().foreach(print)


    sc.stop()
  }

}
