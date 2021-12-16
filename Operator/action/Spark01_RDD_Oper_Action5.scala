package Operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Oper_Action5 {
  def main(args: Array[String]): Unit = {
    // 一个应用程序，Driver程序
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //todo 算子--行动

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2),("c",3),("d",4)), 2)

    //collect是按照分区号码进行采集的
    rdd.collect().foreach(println)


    rdd.foreach(println)

    
    sc.stop()
  }

}
