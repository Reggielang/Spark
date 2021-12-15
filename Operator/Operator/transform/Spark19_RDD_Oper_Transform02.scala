package Operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19_RDD_Oper_Transform02 {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    conf.set("spark.local.dir","d:/test")
    val sc = new SparkContext(conf)

    val rdd: RDD[(User, Int)] = sc.makeRDD(List(
      (new User(), 2), (new User(), 1), (new User(), 3), (new User(), 4)
    ))
    //todo 算子--转换--SortByKey
    //SortByKey 按照Key排序
    val rdd1: RDD[(User, Int)] = rdd.sortByKey(false)

    rdd1.collect().foreach(print)


    sc.stop()
  }

  class User extends Ordered[User]{
    override def compare(that: User): Int = {
      1
    }
  }

}
