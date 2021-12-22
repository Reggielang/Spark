package summer.controller

import summer.common.TController
import summer.service.HotCategorytop10Service

class HotCategorytop10Controller extends TController{

  private val hotCategorytop10Service = new HotCategorytop10Service
  override def dispatch(): Unit = {
    val result = hotCategorytop10Service.analysis()
    result.foreach(println)
  }
}
