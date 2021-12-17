package acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}


object Spark01_Acc02 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //todo 创建累加器 -- 分布式共享只写变量

    val sum: LongAccumulator = sc.longAccumulator("sum")


//    var sum = 0  //Driver

    rdd.foreach(
      num=>{
        //使用累加器
        sum.add(num)
//      sum=sum+num; //Executor
    })

    //获取累加器的结果
    println(sum.value)
//    print(sum) // Driver端

    sc.stop()
  }

}
