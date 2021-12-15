package Operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Oper_Transform {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //todo 算子--转换-- glom 将同一个分区的数据直接转换为相同类型的内存数组进行处理


    val rdd = sc.makeRDD(List(1,2,3,4,5,6),2)

    val rdd1 = rdd.glom()


    rdd1.collect().foreach(a=> println(a.mkString(",")))

    sc.stop()
  }

}
