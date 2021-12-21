package spark_wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object Spark05_WordCount {
  def main(args: Array[String]): Unit = {
      //todo 使用spark
      val conf = new SparkConf().setMaster("local").setAppName("WordCount")
      val sc = new SparkContext(conf)

    //读取文件
    var lines: RDD[String] = sc.textFile("data/word.txt")

    //将文件中的数据进行了分词
    //word=>(word,1)
    val wordsRDD = lines.flatMap(
      list=>{
        val words: Array[String] = list.split(" ")
        words.map(
          word=>{
            //hello=>Map[(hello,1)]
            mutable.Map((word,1))
          }
        )
      }
    )



    //(T1(map),T2(map)) =>T3(map)
    val map: mutable.Map[String, Int] = wordsRDD.reduce(
      (map1, map2) => {
        map2.foreach{
          case(word,cnt)=>{
            val oldcnt: Int = map1.getOrElse(word, 0)
            map1.update(word,oldcnt+cnt)
          }
        }

        map1
      }
    )


    println(map)
    sc.stop()
  }

}
