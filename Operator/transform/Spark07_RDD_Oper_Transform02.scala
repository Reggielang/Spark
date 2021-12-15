package Operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Oper_Transform02 {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    conf.set("spark.local.dir","d:/test")
    val sc = new SparkContext(conf)

    //todo 算子--转换-- filter
    val lines: RDD[String] = sc.textFile("data/apache.log")

    val filterlines: RDD[String] = lines.filter(
      line => {
        //        line.contains("17/05/2015")
        val datas: Array[String] = line.split(" ")
        val times: String = datas(3)
        times.startsWith("17/05/2015")
      }
    )

    val rdd1: RDD[String] = filterlines.map(
      line => {
        val datas: Array[String] = line.split(" ")
        datas(6)
      }
    )

    rdd1.collect().foreach(println)
    sc.stop()
  }

}
