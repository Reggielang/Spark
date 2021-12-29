package SQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object Spark03_SparkSQL_UDAF2 {
  def main(args: Array[String]): Unit = {
    //todo 创建连接环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val df: DataFrame = spark.read.json("data/user.json")
    //在早期版本中，spark不能在sql中使用强类型的udaf操作。
    //SQL&DSL
    //早期udaf强类型聚合函数使用DSL语法操作
    df.createOrReplaceTempView("user")

    import spark.implicits._
    val ds: Dataset[User] = df.as[User]

    //将UDAF函数转换为查询的列对象
    val udafCol: TypedColumn[User, Long] = new MyavgUDAF().toColumn

    ds.select(udafCol).show


    spark.udf.register("ageAvg",functions.udaf(new MyavgUDAF()))

    spark.sql("select ageAvg(age) from user").show

    //todo 关闭环境
    spark.close()
  }
  /*
  自定义聚合函数类：计算年龄的平均值
  1.继承org.apache.spark.sql.expressions.Aggregator
  IN 输入的数据类型 User
  BUF
  OUT 输出的数据类型
  2.重写方法
   */
  case class User(username:String,age:Int)
  case class Buff(var total:Long,var conut:Long)
  class MyavgUDAF extends Aggregator[User,Buff,Long]{
    override def zero: Buff = {
      //初始值或者零值
      // 缓冲区的初始化
      Buff(0L,0L)
    }

    //根据输入的数据更新缓冲区的数据
    override def reduce(buff: Buff, in: User): Buff ={
      buff.total = buff.total+in.age
      buff.conut = buff.conut+1
      buff
    }

    //合并缓冲区
    override def merge(buff1: Buff, buff2: Buff): Buff = {
      buff1.total=buff1.total+buff2.total
      buff1.conut=buff1.conut+buff2.conut
      buff1
    }

    //计算结果
    override def finish(buff: Buff): Long = {
      buff.total/buff.conut
    }
    //缓冲区的编码操作
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    //输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }
}
