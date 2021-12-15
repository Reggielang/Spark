package Operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Oper_Transform02 {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //todo 算子--转换-- glom 计算所有分区最大值求和（分区内取最大值，分区间最大值求和）


    val rdd = sc.makeRDD(List(1,2,3,4,5,6),2)

    val rdd1 = rdd.glom()

    //两个分区就是两个集合，然后集合里面取最大值
    val rdd2 = rdd1.map(_.max)

    //两个最大值相加即可
    println(rdd2.collect().sum)

    sc.stop()
  }

}
