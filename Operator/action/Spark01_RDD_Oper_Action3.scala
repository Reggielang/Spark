package Operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Oper_Action3 {
  def main(args: Array[String]): Unit = {
    // 一个应用程序，Driver程序
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //todo 算子--行动

    val rdd: RDD[Int] = sc.makeRDD(List(1, 1,2,2,2, 3, 4),2)
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 2), ("a", 3)))

    //统计每种key的个数
    val rdd1: RDD[(String, Int)] = rdd.map(("a", _))
    val map: collection.Map[(String, Int), Long] = rdd1.countByValue()

    //countByValue,不是K,V键值对的V
    //countByValue 也可以实现word count

    //把数据扁平化
    //("a",2)=>"a"，"a"
    // ("a",3)=>"a"，"a","a"

    //然后再用countByValue，就可以实现word count




    println(map)




    sc.stop()
  }

}
