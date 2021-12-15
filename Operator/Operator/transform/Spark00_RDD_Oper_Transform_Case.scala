package Operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark00_RDD_Oper_Transform_Case {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    conf.set("spark.local.dir","d:/test")
    val sc = new SparkContext(conf)

    //todo 案例 统计出每一个省份每个广告被点击数量排行的Top3 先统计减少数据量之后再分组
    // reduceByKey有预聚合功能
//    1)数据准备
//    agent.log：时间戳，省份，城市，用户，广告，中间字段使用空格分隔。
//    2)需求描述
//    统计出每一个省份每个广告被点击数量排行的Top3
    //todo 1.获取原始数据
    val lines: RDD[String] = sc.textFile("data/agent.log")
    //  1516609143867 6 7 64 16
    //todo 2.将原始数据进行结构转换
    // k-v
    // line=>(（省份，广告），1)
    val wordToOne: RDD[((String, String), Int)] = lines.map(
      line => {
        val datas: Array[String] = line.split(" ")
        ((datas(1), datas(4)), 1)
      }
    )

    //todo 3.将转换后的结构数据进行统计
    // (（省份，广告），1) =>(（省份，广告），sum)
    val wordToSum: RDD[((String, String), Int)] = wordToOne.reduceByKey(_ + _)

    //todo 4.将统计结果进行格式转换，将省份独立出来
    // (（省份，广告），1) =>(省份(广告，sum))
    val wordToTuple: RDD[(String, (String, Int))] = wordToSum.map {
      case ((prv, adv), sum) => {
        (prv, (adv, sum))
      }
    }

    //todo 5.将数据按照省份进行分组
    // (省份,List[(广告1，sum),(广告2，sum),(广告3，sum)])
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = wordToTuple.groupByKey()

    //todo 6.将分组后的数据，根据点击数量进行排序（降序）
    val top3: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    )

    // todo 7. 将排序后的数据取前三
    top3.collect().foreach(println)





    sc.stop()
  }
}
