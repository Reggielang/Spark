package Case

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
HotCategoryTop10

 */
object Spark01_Case01 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)
    //todo 需求1：热门品类top10

    val fileDatas: RDD[String] = sc.textFile("data/user_visit_action.txt")
    //todo 读取文件，获取原始数据
    //todo 统计品类的点击数量
    //统计分析前，过滤不需要的数据
    // 先保留所有的点击数据

    val clickDatas: RDD[String] = fileDatas.filter(
      data => {
        val datas: Array[String] = data.split("_")
        val cid: String = datas(6)
        //如果点击的品类ID和产品ID为-1，表示数据不是点击数据
        cid != "-1"
      }
    )
    // 对点击数据做统计
    val clickCntDatas: RDD[(String, Int)] = clickDatas.map(
      data => {
        val datas: Array[String] = data.split("_")
        val cid: String = datas(6)
        //某一个品类被点击了一次
        (cid, 1)
      }
    ).reduceByKey(_ + _)

    //todo 统计品类的下单数量
    val orderDatas: RDD[String] = fileDatas.filter(
      data => {
        val datas: Array[String] = data.split("_")
        val cid: String = datas(8)
        cid != "null"
      }
    )
    // 对下单数据做统计
    // (1,2,3,4)
    // (1,1),(2,1),(3,1),(4,1)
    val orderCntDatas: RDD[(String, Int)] = orderDatas.flatMap(
      data => {
        val datas: Array[String] = data.split("_")
        val cid: String = datas(8)
        val cids: Array[String] = cid.split(",")
        cids.map((_, 1))
      }
    ).reduceByKey(_ + _)


    //todo 统计品类的支付数量
    val payDatas: RDD[String] = fileDatas.filter(
      data => {
        val datas: Array[String] = data.split("_")
        val cid: String = datas(10)
        cid != "null"
      }
    )
    // 对支付数据做统计
    // (1,2,3,4)
    // (1,1),(2,1),(3,1),(4,1)
    val payCntDatas: RDD[(String, Int)] = payDatas.flatMap(
      data => {
        val datas: Array[String] = data.split("_")
        val cid: String = datas(10)
        val cids: Array[String] = cid.split(",")
        cids.map((_, 1))
      }
    ).reduceByKey(_ + _)


    //todo 对统计结果进行排序 => tuple(点击，下单，支付)
    //val clickSortedDatas: RDD[(String, Int)] = clickCntDatas.sortBy(_._2, false)

    // (品类id,点击) 点击
    // (品类id,下单) 下单
    // (品类id,支付) 支付
    // (品类id,(点击，下单，支付)
    //join
    //clickCntDatas.fullOuterJoin(orderCntDatas).fullOuterJoin(payCntDatas)

    val cidCntDatas: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] =
      clickCntDatas.cogroup(orderCntDatas, payCntDatas)

    val mapDatas: RDD[(String, (Int, Int, Int))] = cidCntDatas.map {
      case (cid, (clickIter, orderIter, payIter)) => {
        var clickcnt = 0
        var ordercnt = 0
        var paycnt = 0
        val iterator1: Iterator[Int] = clickIter.iterator
        if (iterator1.hasNext) {
          clickcnt = iterator1.next()
        }
        val iterator2: Iterator[Int] = orderIter.iterator
        if (iterator2.hasNext) {
          ordercnt = iterator2.next()
        }
        val iterator3: Iterator[Int] = payIter.iterator
        if (iterator3.hasNext) {
          paycnt = iterator3.next()
        }
        (cid, (clickcnt, ordercnt, paycnt))
      }
    }


    val top10: Array[(String, (Int, Int, Int))] = mapDatas.sortBy(_._2, false).take(10)






    //todo 讲结果采集后打印到控制台
//    clickCntDatas.collect().foreach(println)
//    orderCntDatas.collect().foreach(println)
//    payCntDatas.collect().foreach(println)
    top10.foreach(println)
    sc.stop()
  }

}
