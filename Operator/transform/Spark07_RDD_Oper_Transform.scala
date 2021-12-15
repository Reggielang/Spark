package Operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Oper_Transform {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    conf.set("spark.local.dir","d:/test")
    val sc = new SparkContext(conf)

    //todo 算子--转换-- filter
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6))

    //filter算子可以按照指定的规则对每一条数据进行过滤
    // 数据处理结果为true，表示数据保留，为false数据就丢弃

    val rdd1: RDD[Int] = rdd.filter(
      num => num % 2 == 1
    )

    rdd1.collect().foreach(println)
    sc.stop()
  }

}
