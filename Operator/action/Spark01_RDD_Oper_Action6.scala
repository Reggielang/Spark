package Operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Oper_Action6 {
  def main(args: Array[String]): Unit = {
    // 一个应用程序，Driver程序
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //todo 算子--行动

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

    val user = new User() //Driver

    rdd.foreach(
      num=>{
        println(user.age+num) //Executor
      }
    )

    //Scala语法：闭包
    //spark在执行算子时，如果的算子的内部使用了外部的变量（对象），那么意味着一定会出现闭包
    //在这种场景中，需要将driver端，通过网络传入给executor端，这个操作不用执行也能判断出来
    //可以在真正执行之前，对数据进行序列化校验

    // spark在执行作业前，需要先进行闭包检测功能！

    sc.stop()
  }

  class User extends Serializable {
    val age =30
  }
}
