package Operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Oper_Transform {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    conf.set("spark.local.dir","d:/test")
    val sc = new SparkContext(conf)

    //todo 算子--转换-- sortBy 排序
    val rdd: RDD[Int] = sc.makeRDD(List(1, 3, 5, 4, 2),2)

    val rdd1: RDD[Int] = rdd.sortBy(num => num,false)

    rdd1.collect().foreach(println)



    sc.stop()
  }

}
