package spark_wordcount

import org.apache.spark.{SparkConf, SparkContext}


object Spark01_WordCount_Env {
  def main(args: Array[String]): Unit = {
      //todo 使用spark
      //Spark是一个计算框架
    //1.找到spark：增加依赖
    //2.获取spark的连接（环境）先配置环境的参数
      val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
      val sc = new SparkContext(conf)

    //读取文件
    val lines = sc.textFile("data/word.txt")

    //将文件中的数据进行了分词
    val words = lines.flatMap(_.split(" "))

    //将分词后的数据进行分组
    val wordGroup = words.groupBy(word => word)

    //对分组后的数据进行统计分析
    val wordCount = wordGroup.mapValues(_.size)

    //将结果打印在控制台
    wordCount.collect().foreach(println)

    sc.stop()
  }

}
