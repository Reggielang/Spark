package acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark01_Acc {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    var sum = 0  //Driver

    rdd.foreach(
      num=>{
      sum=sum+num; //Executor
    })

    print(sum) // Driver端

    sc.stop()
  }

}
