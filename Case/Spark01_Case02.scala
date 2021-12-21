package Case

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
HotCategoryTop10

 */
object Spark01_Case02 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)
    //todo 需求1：热门品类top10 --改版1
    //1.同一个RDD重复使用
    //2.cogroup算子可能性能低下

    val fileDatas: RDD[String] = sc.textFile("data/user_visit_action.txt")
    //缓存一下
    fileDatas.cache()

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

    // (品类id,点击) => (品类id,(点击，0，0))
    // (品类id,下单) => (品类id,(0，下单，0))
    // (品类id,支付) => (品类id,(0，0，支付))
    // (品类id,(点击，下单，支付)
    // 3=>1=>(聚合)
    // reduceByKey
    //((点击，下单，支付),(点击，下单，支付)) => (点击，下单，支付)
    //clickCntDatas.fullOuterJoin(orderCntDatas).fullOuterJoin(payCntDatas)
    val clickMapDatas = clickCntDatas.map {
      case (cid, cnt) => {
        (cid, (cnt, 0, 0))
      }
    }

    val orderMapDatas = orderCntDatas.map {
      case (cid, cnt) => {
        (cid, (0, cnt, 0))
      }
    }

    val payMapDatas = payCntDatas.map {
      case (cid, cnt) => {
        (cid, (0, 0, cnt))
      }
    }

    val unionRDD: RDD[(String, (Int, Int, Int))] = clickMapDatas.union(orderMapDatas).union(payMapDatas)

    val reduceRDD: RDD[(String, (Int, Int, Int))] = unionRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    val top10: Array[(String, (Int, Int, Int))] = reduceRDD.sortBy(_._2, false).take(10)


    //todo 讲结果采集后打印到控制台
//    clickCntDatas.collect().foreach(println)
//    orderCntDatas.collect().foreach(println)
//    payCntDatas.collect().foreach(println)
    top10.foreach(println)
    sc.stop()
  }

}
