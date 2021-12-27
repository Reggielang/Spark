package SQL

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Spark01_SparkSQL_Basic {
  def main(args: Array[String]): Unit = {
    //todo 创建连接环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    //todo 执行逻辑操作
    //dataframe
    val df: DataFrame = spark.read.json("data/user.json")
    df.show()

    //dataframe=>SQL
    df.createOrReplaceTempView("user")

    spark.sql("select * from user").show
    spark.sql("select avg(age) from user").show


    //dataframe=>DSL
    //在使用dataframe时，如果涉及到转换操作，需要引入转换规则

    df.select("age","username").show

    df.select($"age"+1).show
    df.select('age+1).show

    //todo dataset
    //dataframe其实是特定泛型的dataset
    val seq = Seq(1,2,3,4)
    val ds: Dataset[Int] = seq.toDS()
    ds.show()


    //rdd<=>dateframe
    val rdd = spark.sparkContext.makeRDD(List((1,"zhangsan",29),(2,"lisi",40)))

    val df2: DataFrame = rdd.toDF("id", "name", "age")

    val rowRDD = df2.rdd

    //dateframe<=>dataset
    val ds2: Dataset[User] = df2.as[User]
    val df3: DataFrame = ds2.toDF()

    //rdd<=>dataset
    val ds3: Dataset[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()

    val UserRDD: RDD[User] = ds3.rdd


    //todo 关闭环境
    spark.close()
  }
  case class User(id:Int,name:String,age:Int)
}
