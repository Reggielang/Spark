package Operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark20_RDD_Oper_Transform {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    conf.set("spark.local.dir","d:/test")
    val sc = new SparkContext(conf)

    //todo 算子--转换--Join
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 2), ("b", 1), ("c", 3),
    ))

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 2), ("a", 1), ("a", 3),
    ))

    //join =>spark中的join主要针对于相同的Key的数据连接
    //join操作可能会产生笛卡尔积，可能会出现shuffle，性能比较差。所以不推荐使用join
    val rdd3: RDD[(String, (Int, Int))] = rdd.join(rdd1)
    rdd3.collect().foreach(println)

    // 主，从表
    val rdd4: RDD[(String, (Int, Option[Int]))] = rdd.leftOuterJoin(rdd1)
    val rdd5: RDD[(String, (Option[Int], Int))] = rdd.rightOuterJoin(rdd1)
    val rdd6: RDD[(String, (Option[Int], Option[Int]))] = rdd.fullOuterJoin(rdd1)
//    rdd4.collect().foreach(println)
//    rdd5.collect().foreach(println)
//    rdd6.collect().foreach(println)

    //cogroup
    println("*********************")
    val rdd7: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd.cogroup(rdd1)
    rdd7.collect().foreach(println)




    sc.stop()
  }
}
