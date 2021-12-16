package Operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Oper_Action2 {
  def main(args: Array[String]): Unit = {
    // 一个应用程序，Driver程序
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //todo 算子--行动

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    //统计每种key的个数


    val rdd1: RDD[(String, Int)] = rdd.map(("a", _))
    //(a,1),(a,2)

    //countByKey也可以做word count
    val map: collection.Map[String, Long] = rdd1.countByKey()
    //a->3

    println(map)




    sc.stop()
  }

}
