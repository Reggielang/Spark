package SQL

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object Spark01_SparkSQL_JDBC {
  def main(args: Array[String]): Unit = {
    //todo 创建连接环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    // 读取Mysql数据
    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user")
      .load()

      df.show

    //保存数据
    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user1")
      .mode(SaveMode.Append)
      .save()
    //todo 关闭环境
    spark.close()
  }
  case class User(id:Int,name:String,age:Int)
}
