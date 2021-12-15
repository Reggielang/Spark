package Operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Oper_Transform {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //todo 算子--转换-- mapPartitions
    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)


    val rdd1 = rdd.mapPartitions(
      list => {
        println("******************")
        list.map(_*2)
      }
    )

    rdd1.collect().foreach(println)

    sc.stop()
  }

}
