package Operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Oper_Transform02 {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //todo 算子--转换-- mapPartitions --小练习
    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)
    // 获取每个分区的最大值

    val rdd1 = rdd.mapPartitions(
      list => {
        val max = list.max
        List(max).iterator
      }
    )

    rdd1.collect().foreach(println)

    sc.stop()
  }

}
