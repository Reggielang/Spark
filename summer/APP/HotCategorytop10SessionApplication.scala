package summer.APP

import summer.common.TApp
import summer.controller.{HotCategorytop10Controller, HotCategorytop10SessionController}

/*
controller:调度器，调度对象之间的关系

服务：逻辑服务

DAO:data access object

 */

object HotCategorytop10SessionApplication extends TApp with App {

  execute(appName = "HotCategoryTop10Session"){
    val controller = new HotCategorytop10SessionController
    controller.dispatch()
  }

}
