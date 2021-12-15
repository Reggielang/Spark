package Operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Oper_Transform01 {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //todo 算子--转换-- map
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    //分区？ 在RDD进行转换时，新的RDD和旧的RDD分区数量保持一致
    //数据执行顺序 数据处理过程中，遵循分区内有序，分区间无序

    val rdd1 = rdd.map(
      num=>{
        println("num = "+num)
        num*2
      }
    )


    val rdd2 = rdd1.map(
      num=>{
        println("num = "+num)
        num*2
      }
    )


    rdd1.saveAsTextFile("output1")
    sc.stop()
  }

}
