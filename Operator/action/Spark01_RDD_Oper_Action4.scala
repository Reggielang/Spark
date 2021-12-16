package Operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Oper_Action4 {
  def main(args: Array[String]): Unit = {
    // 一个应用程序，Driver程序
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //todo 算子--行动

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2)), 2)

    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output1")
    rdd.saveAsSequenceFile("output2")

    sc.stop()
  }

}
