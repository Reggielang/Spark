package SQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark04_SparkSQL_Hive {
  def main(args: Array[String]): Unit = {


    //todo 创建连接环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")

    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()


    //使用sparkSQL连接外置的Hive
    //拷贝hive-site.xml文件到resources目录
    //启用hive的支持
    //增加对应的依赖关系（包含Mysql的驱动）
    spark.sql("show tables").show


    spark.close()
  }

}
