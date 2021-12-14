package RDD_test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Instance_Memory_Partitior {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    conf.set("spark.default.parallelism","4")
    val sc = new SparkContext(conf)

    //todo 从内存中创建RDD --分区
    //如果构建RDD时没有指定数据处理分区的数量，那么会使用默认分区的数量
    //makeRDD方法存在第二个参数，这个参数表示分区数量有默认值 numSlices
    //scheuler.conf.getInt("spark.default.parallelism",totalCores)
    //totalCores:当前Master环境的总（虚拟）核数
    //分区设置优先度：方法参数>配置参数>环境配置

    //kafka生产者分区策略
    //[1,3] [2,4] :轮询
    //[1,2] [3,4] :范围

    //spark分区策略
    //[1,2] [3,4] :范围
    val rdd1: RDD[Int] = sc.makeRDD(
      Seq(1, 2, 3, 4,5),2
    )

    //saveAsTextFile方法可以生成分区文件
    rdd1.saveAsTextFile("D:/大数据相关学习/spark_test/output")


    sc.stop()
  }

}
