package acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object Spark01_Bc {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(
      ("a",1),("b",2)
    ))

    //todo 广播变量 --分布式共享只读变量
    // 如果不用广播变量，那么在Executor端执行任务时，
    // 每一个数据都会去拉取一个Driver端的map变量，那么会造成大量的数据冗余
    
    val map =mutable.Map[String,Int](
      ("c",3),("d",4)
    )
    //分布式共享只读变量
    val bcMap: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

    val rdd2: RDD[(String, (Int, Int))] = rdd.map {
      case (word, cnt) => {
        val cnt2 = bcMap.value.getOrElse(word,0)
        (word, (cnt, cnt2))
      }
    }

    rdd2.collect().foreach(println)

  }
}
