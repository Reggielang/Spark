package RDD_test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Instance_File_Partitior {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //todo 从文件中创建RDD
    //textFile可以在读取文件时。设定分区
    //设定分区时，应该传递第二个参数，如果不设定存在默认值
    // math.min(defaultParallelism,2)
    //第二个参数表示最小分区数，所以最终的分区数量可以大于这个值的。

    //todo 1.spark读取文件，其实底层就是Hadoop读取文件
    // 2.spark的分区数量其实就是Hadoop读取文件的切片
    // （想要的切片数量）numSplits=2
    // totalSize = 5
    // (预计每个分区的字节大小) goalSize = totalSize/numSplits = 5/2=2
    // 5/2 =2...1 = 2+1 = 3 (剩余的size，会除以想要的切片数量，如果剩余10%以上，则多一个分区)
    //假设你有10G的数据，然后你想要5G一个分区，但实际会进行下面的判断，之后，切片数量为10G/128M
    //splitSize = Math.max(minSize(1),Math.min(goalSize(5G), blockSize(128M))) = 128M
     val rdd: RDD[String] = sc.textFile("D:/大数据相关学习/spark_test/data/word.txt",3)

    rdd.saveAsTextFile("D:/大数据相关学习/spark_test/output")

    sc.stop()
  }

}
