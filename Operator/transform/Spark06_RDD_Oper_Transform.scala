package Operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Oper_Transform {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    conf.set("spark.local.dir","d:/test")
    val sc = new SparkContext(conf)

    //todo 算子--转换-- groupBy


    val rdd = sc.makeRDD(List(1,2,3,4,5,6))
    //groupby 算子根据函数计算结果分组
    // groupby 算子执行结果为KV数据类型 K为分组的标识，V就是同一个组的数据集合
    val rdd1: RDD[(Int, Iterable[Int])] = rdd.groupBy(_ % 2)

    sc.stop()
  }

}
