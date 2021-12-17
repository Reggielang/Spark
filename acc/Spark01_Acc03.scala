package acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object Spark01_Acc03 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List(
      "scala",
      "scala",
      "scala",
      "scala",
      "scala",
      "scala",
      "spark",
    ))

    //todo 创建累加器 -- 分布式共享只写变量
    val acc = new WordCountAccumulator()

    //向spark注册
    sc.register(acc,"WordCount")

    rdd.foreach(
      word=>{
        //todo 单词放入累加器中
        acc.add(word)
      }
    )
    //todo 获取累加器的累加结果
    println(acc.value)


    sc.stop()
  }
  //todo 自定义数据累加器
  //1.继承
  //2.定义泛型
  //IN:String
  //out:map[k,v]
  //重写方法 3+3
  class WordCountAccumulator extends AccumulatorV2[String,mutable.Map[String,Int]]{

    private val wcMap = mutable.Map[String,Int]()

    //判断累加器是否是初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
      new WordCountAccumulator()
    }

    //重置累加器
    override def reset(): Unit = {
      wcMap.clear()
    }

    //从外部向累加器中加入数据
    override def add(word: String): Unit = {
      val oldcnt = wcMap.getOrElse(word, 0)
      wcMap.update(word,oldcnt+1)
    }

    //合并两个累加器的结果
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
      other.value.foreach{
        case (word,cnt)=>{
          val oldcnt: Int = wcMap.getOrElse(word, 0)
          wcMap.update(word,oldcnt+cnt)
        }
      }
    }

    // 将结果返回到外部
    override def value: mutable.Map[String, Int] = wcMap

  }
}
