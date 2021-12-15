package Operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_RDD_Oper_Transform {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    conf.set("spark.local.dir","d:/test")
    val sc = new SparkContext(conf)

    //todo 算子--转换--K.V类型
    // partitionBy 根据指定的规则对每一条数据进行重分区
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val rdd1: RDD[(Int, Int)] = rdd.map((_, 1))
    //rdd=>PairRddFunction
    //HashPartitioner是spark中默认的shuffle分区器
    rdd1.partitionBy(new HashPartitioner(2)).saveAsTextFile("output2")




    sc.stop()
  }

}
