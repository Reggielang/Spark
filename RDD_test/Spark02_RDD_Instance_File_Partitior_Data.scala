package RDD_test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Instance_File_Partitior_Data {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //todo 从文件中创建RDD
    // 1.分区数据的处理也是由Hadoop处理的
    // 2.hadoop在计算分区时和处理数据时的逻辑不一样
    // 3.spark读取文件数据的底层使用的就是Hadoop读取的，所以读取规则用的是Hadoop
    // 3.1Hadoop读取数据是按行读取，不是按字节读取
    // 3.2hadoop读取数据是按偏移量读取的
    // 3.3Hadoop读取数据时，不会读取重复的偏移量

    /*
    1@@ =>012
    2@@ =>345
    3 => 6

    计算读取偏移量
    【0，3】=》【12】
    【3，6】=》【6】
    【6，7】=》【】

    123@@ =》01234
    456@@ =》56789
    789 =》101112

    计算有多少个分区
    13字节（文件总字节数）/3（指定的分区数量） = 4 =》一个分区4个字节

    13/4...1 = 3+1=4个分区

    计算每个分区放什么数据
    偏移量 =》 读取到的数据
    【0，4】=》 【123】
    【4，8】 =》 【456】
    【8，12】 =》【789】
    【12，13】 =》【】
     */
    val rdd: RDD[String] = sc.textFile("D:/大数据相关学习/spark_test/data/word.txt",3)

    rdd.saveAsTextFile("D:/大数据相关学习/spark_test/output")

    sc.stop()
  }

}
