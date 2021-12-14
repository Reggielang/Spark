package Operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Oper_Transform {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //todo 算子--转换-- map
    val rdd = sc.makeRDD(List(1,2,3,4))

    //map算子表示将数据源中的每条数据进行处理
    //map算子的参数是函数类型，Int=>U(不确定)
    def mapFunction(num:Int)={
      num*2
    }

    //val rdd1 = rdd.map(mapFunction)
    val rdd1 = rdd.map(_*2)

    rdd1.collect().foreach(println)
    sc.stop()
  }

}
