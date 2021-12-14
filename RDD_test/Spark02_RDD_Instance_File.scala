package RDD_test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Instance_File {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //todo 从文件中创建RDD
    //textFile方法可以通过路径获取数据，所以可以将文件作为数据处理的数据源
    //textFile的路径可以是相对路径，也可以是绝对路径，也可以是HDFS路径,也可以是一个目录路径，还可以为通配符路径

     val rdd: RDD[String] = sc.textFile("data/word.txt")

    //如果读取文件后，想要获取文件数据的来源用这个方法wholeTextFiles
    var rdd1: RDD[(String, String)] = sc.wholeTextFiles("data/word*.txt")
    rdd.collect().foreach(println)
    print("*********************************")
    rdd1.collect().foreach(println)

    sc.stop()
  }

}
