package Operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Oper_Transform02 {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    conf.set("spark.local.dir","d:/test")
    val sc = new SparkContext(conf)

    //todo 算子--转换-- groupBy
    //shuffle操作不允许在内存中等待，必须落盘
    // （shuffle会将完整的计算过程一分为二，形成两个阶段，一阶段用于写数据，二阶段用于读数据，
    // 写数据阶段如果没有完成，读数据阶段不能执行）
    //shuffle操作可以更改分区
    //练习1
//    val rdd = sc.makeRDD(List("Hello", "hive", "hbase", "Hadoop"))
//
//    val rdd1: RDD[(String, Iterable[String])] = rdd.groupBy(_.substring(0, 1))
//
//    rdd1.collect().foreach(println)

    //从服务器日志数据中获取每个时间段的访问量
    // (10,101)
    val lines = sc.textFile("data/apache.log")

    //(time,List((time,1),(time,2))
    //groupBy 算子可以实现WordCount(1/10)
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = lines.map(
      lines => {
        val datas: Array[String] = lines.split(" ")
        val time: String = datas(3)
        val hour: Array[String] = time.split(":")
        (hour(1), 1)
      }
    ).groupBy(_._1)

    val timecnt: RDD[(String, Int)] = groupRDD.mapValues(_.size)

    timecnt.collect().foreach(println)

    sc.stop()
  }

}
