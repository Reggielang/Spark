package Operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_RDD_Oper_Transform {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    conf.set("spark.local.dir","d:/test")
    val sc = new SparkContext(conf)

    //todo 算子--转换-- sample
    val rdd: RDD[Int] = sc.makeRDD(1 to 10)

    //抽取数据--采样
    // 第一个参数：抽取数据的方式 true.抽取放回 false.抽取不放回
    // 第二个参数和第一个参数有关系：如果抽取不放回的场合：参数表示每条数据被抽取的概率
    // 第二个参数和第一个参数有关系：如果抽取放回的场合：参数表示每天数据希望被重复抽取的次数
    // 第三个参数 随机数种子
    val rdd1: RDD[Int] = rdd.sample(false, 0.5,1)
//    val rdd1: RDD[Int] = rdd.sample(true, 2)

    rdd1.collect().foreach(println)
    sc.stop()
  }

}
