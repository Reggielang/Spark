package summer.service

import org.apache.spark.rdd.RDD
import summer.DAO.{HotCategorytop10Dao, HotCategorytop10SessionDao}
import summer.bean.UserVisitAction
import summer.common.TService

class HotCategorytop10SessionService extends TService{
  private val hotCategorytop10SessionDao = new HotCategorytop10SessionDao

  override def analysis(data:Any)= {
    val topids: Array[String] = data.asInstanceOf[Array[String]]
    val fileDatas: RDD[String] = hotCategorytop10SessionDao.readFileBySpark("data/user_visit_action.txt")
    val actionDatas: RDD[UserVisitAction] = fileDatas.map(
      data => {
        val datas: Array[String] = data.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )
      }
    )

//    println(actionDatas.count())
    val clickDatas: RDD[UserVisitAction] = actionDatas.filter(
      data => {
        if (data.click_category_id != -1) {
          println("xxxxxxx")
          topids.contains(data.click_category_id.toString)
        } else {
          false
        }
      }
    )
//    println(clickDatas.count())
    val reduceDatas: RDD[((Long, String), Int)] = clickDatas.map(
      data => {
        ((data.click_category_id, data.session_id), 1)
      }
    ).reduceByKey(_ + _)

    val groupDatas: RDD[(Long, Iterable[(String, Int)])] = reduceDatas.map {
      case ((cid, sid), cnt) => {
        (cid, (sid, cnt))
      }
    }.groupByKey()

    groupDatas.mapValues(
      iter=>{
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
      }
    ).collect()


  }
}
