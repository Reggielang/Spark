package Operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark18_RDD_Oper_Transform02 {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    conf.set("spark.local.dir","d:/test")
    val sc = new SparkContext(conf)

    //todo 算子--转换--combineByKey
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ), 2)


    //combineByKey算子也可以实现word count
    val rdd2: RDD[(String, Int)] = rdd.combineByKey(
      num => num,
      (x: Int, y) => {
        x + y
      },
      (x: Int, y: Int) => {
        x + y
      }
    )

    //wordcount
    rdd.reduceByKey(_+_)
    rdd.aggregateByKey(0)(_+_,_+_)
    rdd.foldByKey(0)(_+_)


    rdd2.collect().foreach(print)


    sc.stop()
  }

}
