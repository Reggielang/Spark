package SQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark04_SparkSQL_Hive2 {
  def main(args: Array[String]): Unit = {


    //todo 创建连接环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")

    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    spark.sql("create database spark_test")
    spark.sql("use spark_test")

    //准备数据
    spark.sql(
      """
        |CREATE TABLE `user_visit_action`(
        |  `date` string,
        |  `user_id` bigint,
        |  `session_id` string,
        |  `page_id` bigint,
        |  `action_time` string,
        |  `search_keyword` string,
        |  `click_category_id` bigint,
        |  `click_product_id` bigint,
        |  `order_category_ids` string,
        |  `order_product_ids` string,
        |  `pay_category_ids` string,
        |  `pay_product_ids` string,
        |  `city_id` bigint)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)

    spark.sql(
      """
        |load data local inpath 'data/user_visit_action.txt' into table spark_test.user_visit_action
        |""".stripMargin)


    spark.sql(
      """
        |CREATE TABLE `product_info`(
        |  `product_id` bigint,
        |  `product_name` string,
        |  `extend_info` string)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)
    spark.sql(
      """
        |load data local inpath 'data/product_info.txt' into table spark_test.product_info
        |""".stripMargin)

    spark.sql(
      """
        |CREATE TABLE `city_info`(
        |  `city_id` bigint,
        |  `city_name` string,
        |  `area` string)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)
    spark.sql(
      """
        |load data local inpath 'data/city_info.txt' into table spark_test.city_info
        |""".stripMargin)



    spark.close()
  }

}
