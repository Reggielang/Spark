package Operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Oper_Transform {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //todo 算子--转换-- mapPartitionsWithIndex(分区，集合)

    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)
    //[1,2],[3,4],[5,6]

    val rdd1 = rdd.mapPartitionsWithIndex(
      (ind, list) => {
        if (ind == 1) {
          list
        } else {
          Nil.iterator
        }
      }
    )

    rdd1.collect().foreach(println)

    sc.stop()
  }

}
