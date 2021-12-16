package serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Serial {
  def main(args: Array[String]): Unit = {
    // 一个应用程序，Driver程序
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //todo 序列化
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Spark", "Scala"))

    val s = new Search("S")
    s.filterByQuery(rdd).foreach(println)
    sc.stop()
  }
  case class Search(q:String) {
    def filterByQuery(rdd: RDD[String]):RDD[String]={
      // 算子外-> Driver
      //算子内-> Executor
      rdd.filter(_.startsWith(q))
    }
  }
}
