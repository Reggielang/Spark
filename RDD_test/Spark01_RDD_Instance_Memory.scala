package RDD_test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Instance_Memory {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //todo 从内存中创建RDD
    //parallelize方法，用于构建RDD，也可以将这个集合当成数据模型处理的数据源

    //parallelize单词表示并行
    val rdd: RDD[Int] = sc.parallelize(
      Seq(1, 2, 3, 4)
    )
    val rdd1: RDD[Int] = sc.makeRDD(
      Seq(1, 2, 3, 4)
    )


    sc.stop()
  }

}
