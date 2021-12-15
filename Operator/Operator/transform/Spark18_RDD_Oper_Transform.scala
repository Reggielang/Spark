package Operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark18_RDD_Oper_Transform {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    conf.set("spark.local.dir","d:/test")
    val sc = new SparkContext(conf)

    //todo 算子--转换--conbineByKey
    // 将数据List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))求每个key的平均值
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ), 2)

    //combineByKey算子
    //第一个参数：当第一个数据不符合规则时，用于进行转换的操作
    //第二个参数：分区内计算规则
    //第三个参数：分区间计算规则
    val rdd2: RDD[(String, (Int, Int))] = rdd.combineByKey(
      num => (num, 1),
      (x: (Int, Int), y) => {
        (x._1 + y, x._2 + 1)
      },
      (x: (Int, Int), y: (Int, Int)) => {
        (x._1 + y._1, x._2 + y._2)
      }
    )

    rdd2.collect().foreach(print)


    sc.stop()
  }

}
