package Operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Oper_Action {
  def main(args: Array[String]): Unit = {
    // 一个应用程序，Driver程序
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //todo 算子--行动
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    //spark行动算子被调用时，会触发spark任务的执行
    // collect()算子就是行动算子
    // 行动算子执行时，会构建新的作业
    //reduce算子，注意算子都是分布式的，也就是说假如是两个分区，那么就是分区内处理1+2，3+4，然后再分区间处理
    val i: Int = rdd.reduce(_ + _)

    println(i)

    //collect采集：将数据从Executor端采集回Driver端
    // collect会将数据全部拉取到Driver端的内存中，形成数据结果，可能会导致内存溢出
    val ints: Array[Int] = rdd.collect()

    //count计数
    val l: Long = rdd.count()

    //first返回RDD的第一个元素
    val i1: Int = rdd.first()

    //take由RDD的前n个元素组成的数组
    val ints1: Array[Int] = rdd.take(2)

    //RDD排序后的前n个元素组成的数组
    val ints2: Array[Int] = rdd.takeOrdered(2)

    //分区的数据通过初始值和分区内的数据进行聚合，然后再和初始值进行分区间的数据聚合
    //aggregateByKey是转换算子
    //aggregate是行动算子
    //aggregateByKey执行计算时，初始值只会参与分区内计算
    //aggregate执行计算时，初始值会参与分区内计算一击分区间计算
    val i2: Int = rdd.aggregate(0)(_ + _, _ + _)

    println(i2)

    //fold折叠操作，aggregate的简化版操作=>分区间和分区内计算规则相同
    val i3: Int = rdd.fold(0)(_ + _)

    println(i3)




    sc.stop()
  }

}
