package Operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Oper_Transform {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //todo 算子--转换-- 扁平化

    val rdd = sc.makeRDD(List("Hello Spark","Hello Scala"))

    //整体=》个体
    val rdd1 = rdd.flatMap(_.split(" "))

//    rdd.flatMap(
//      str=>{//整体（1）
//        // 容器（个体（N））
//        str.split(" ")
//      }
//    )


    rdd1.collect().foreach(println)

    sc.stop()
  }

}
