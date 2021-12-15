package Operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Oper_Transform02 {
  def main(args: Array[String]): Unit = {
    //获取spark的连接（环境）先配置环境的参数
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //todo 算子--转换-- map
    //从服务器日志数据中获取用户请求URL资源路径
    val lineRDD = sc.textFile("data/apache.log")

    //long string=>short string


    val urlRDD = lineRDD.map(
      line => {
        val datas = line.split(" ")
        datas(6)
      }
    )

    urlRDD.collect().foreach(println)


    sc.stop()
  }

}
