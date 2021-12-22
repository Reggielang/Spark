package summer.controller

import summer.common.TController
import summer.service.WordCountService

class WordCountController extends TController{
  private val WordCountService = new WordCountService

  //调度
  def dispatch():Unit={
    val wordcount = WordCountService.analysis()
    //todo 5.将统计的结果打印
    println(wordcount)
  }


}
