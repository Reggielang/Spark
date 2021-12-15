package Operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Oper_Transform {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    conf.set("spark.local.dir","d:/test")
    val sc = new SparkContext(conf)

    //todo 算子--转换--aggregateByKey

    //todo 取出每个分区内相同key的最大值，然后分区间相加
    //[(a,1),(a,2),(b,3)] => [(a,2),(b,3)]
    //[(b,4),(b,5),(a,6)] => [(b,5),(a,6)]

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ), 2)
    //aggregateByKey存在函数柯里化

    // 第一个参数列表中有一个参数
    //参数为零值，表示计算初始值zero，z 用于数据进行分区内计算，因为数据是两两计算，当分区内数据只出现一次时，没办法进行两两计算。
    // 第二个参数列表中有二个参数
    //第一个参数:表示分区内的计算规则
    //第二个参数：表示分区间的计算规则
    val rdd1: RDD[(String, Int)] = rdd.aggregateByKey(0)(
      (x, y) => {
        math.max(x, y)
      },
      (x, y) => {
        x + y
      }
    )

    //aggregateByKey 也可以实现word count
    val rdd2: RDD[(String, Int)] = rdd.aggregateByKey(0)(_+_,_+_)


    //todo 如果aggregateByKey算子的分区内和分区间的计算逻辑相同，可以使用foldByKey
    val rdd3: RDD[(String, Int)] = rdd.foldByKey(0)(_+_)
    rdd1.collect().foreach(print)
    rdd2.collect().foreach(print)
    rdd3.collect().foreach(print)


    sc.stop()
  }

}
