package Operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark00_RDD_Oper_Transform_Case02 {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    conf.set("spark.local.dir","d:/test")
    val sc = new SparkContext(conf)

    //todo 案例 统计出每一个省份每个广告被点击数量排行的Top3 先分组再统计
    // 性能比较底，因为要先分组，就会进行大量的shuffle操作
//    1)数据准备
//    agent.log：时间戳，省份，城市，用户，广告，中间字段使用空格分隔。
//    2)需求描述
//    统计出每一个省份每个广告被点击数量排行的Top3
    //todo 1.获取原始数据
    val lines: RDD[String] = sc.textFile("data/agent.log")
    //  1516609143867 6 7 64 16
    //todo 2.将原始数据进行结构转换
    // k-v
    // line=>(省份,(广告,1))
    val wordToOne = lines.map(
      line => {
        val datas= line.split(" ")
        (datas(1),(datas(4), 1))
      }
    )

    val groupRDD: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupByKey()

    val top3: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        val wordCnt: Map[String, Int] = iter.groupBy(_._1).mapValues(_.size)
        wordCnt.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )

    top3.collect().foreach(println)

    sc.stop()
  }
}
