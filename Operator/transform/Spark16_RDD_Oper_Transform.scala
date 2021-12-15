package Operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_RDD_Oper_Transform {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    conf.set("spark.local.dir","d:/test")
    val sc = new SparkContext(conf)

    //todo 算子--转换--groupByKey
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 1), ("a", 1)))

    //groupByKey将相同Key的value分在一个组中
    //todo groupByKey也可以做word count
    val rdd1: RDD[(String, Iterable[Int])] = rdd.groupByKey()

    val rdd2: RDD[(String, Int)] = rdd1.mapValues(_.size)


    // groupByKey groupBy
    //groupBy 不需要考虑数据的类型 groupbykey必须保证数据是K,V类型
    // groupBy 按照指定的规则分组，groupbykey必须根据Key对value进行分组
    // 返回结果类型：groupbyKey=>(String, Iterable[Int])
    // 返回结果类型：groupby=>(String, Iterable[(String, Int)])

    rdd2.collect().foreach(print)


    sc.stop()
  }

}
