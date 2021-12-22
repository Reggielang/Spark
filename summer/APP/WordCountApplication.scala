package summer.APP

import summer.common.TApp
import summer.controller.WordCountController

/*
controller:调度器，调度对象之间的关系

服务：逻辑服务

DAO:data access object

 */

object WordCountApplication extends TApp with App {

  execute(appName = "WordCount"){
    val controller = new WordCountController
    controller.dispatch()
  }

}
