package Case

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io

/*
HotCategoryTop10

 */
object Spark01_Case03 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)
    //todo 需求1：热门品类top10 --改版2
    //1.同一个RDD重复使用
    //2.cogroup算子可能性能低下
    //3.reduceByKey 有4次也就是4次shuffle（4次落盘操作）性能不高。

    val fileDatas: RDD[String] = sc.textFile("data/user_visit_action.txt")
    //缓存一下

    //todo 读取文件，获取原始数据
    //todo 统计品类的点击数量
    //统计分析前，过滤不需要的数据
    // 先保留所有的点击数据

    val flatDatas = fileDatas.flatMap(
      data => {
        var datas = data.split("_")
        if (datas(6) != "-1") {
          //点击数据场合
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          //下单数据changhe
          val id = datas(8)
          val ids: Array[String] = id.split(",")
          ids.map(
            id => {
              (id, (0, 1, 0))
            }
          )
        } else if (datas(10) != "null") {
          //支付数据的场合
          val id = datas(10)
          val ids: Array[String] = id.split(",")
          ids.map(
            id => {
              (id, (0, 0, 1))
            }
          )
        } else {
          Nil
        }
      }
    )

    val top10: Array[(String, (Int, Int, Int))] = flatDatas.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    ).sortBy(_._2, false).take(10)

    //todo 对统计结果进行排序 => tuple(点击，下单，支付)
    //val clickSortedDatas: RDD[(String, Int)] = clickCntDatas.sortBy(_._2, false)

    // (品类id,点击) => (品类id,(1，0，0))
    // (品类id,点击) => (品类id,(1，0，0))
    // (品类id,点击) => (品类id,(1，0，0))

    // (品类id,点击) => (品类id,(sum，0，0))
    // (品类id,下单) => (品类id,(0，sum，0))
    // (品类id,支付) => (品类id,(0，0，sum))
    // (品类id,(点击，下单，支付)
    // 3=>1=>(聚合)
    // reduceByKey
    //((点击，下单，支付),(点击，下单，支付)) => (点击，下单，支付)
    //clickCntDatas.fullOuterJoin(orderCntDatas).fullOuterJoin(payCntDatas)




    //todo 讲结果采集后打印到控制台
//    clickCntDatas.collect().foreach(println)
//    orderCntDatas.collect().foreach(println)
//    payCntDatas.collect().foreach(println)
    top10.foreach(println)
    sc.stop()
  }

}
