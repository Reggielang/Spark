package summer.controller

import summer.common.TController
import summer.service.{HotCategorytop10Service, HotCategorytop10SessionService}

class HotCategorytop10SessionController extends TController{

  private val HotCategorytop10Service = new HotCategorytop10Service
  private val hotCategorytop10SessionService = new HotCategorytop10SessionService

  override def dispatch(): Unit = {
    val top10 = HotCategorytop10Service.analysis()
    val result: Array[(Long, List[(String, Int)])] = hotCategorytop10SessionService.analysis(top10.map(_._1))
    result.foreach(println)
  }
}
