package summer.service

import org.apache.spark.rdd.RDD
import summer.DAO.HotCategorytop10Dao
import summer.common.TService

class HotCategorytop10Service extends TService{
  private val hotCategorytop10Dao = new HotCategorytop10Dao

  override def analysis()= {
    val fileDatas: RDD[String] = hotCategorytop10Dao.readFileBySpark("data/user_visit_action.txt")
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
    top10
  }
}
