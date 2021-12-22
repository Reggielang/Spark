package summer.service

import summer.DAO.WordCountDao
import summer.common.TService

class WordCountService extends TService{
  private val WordCountDao = new WordCountDao
  override def analysis()={

    val lines = WordCountDao.readFile("data/word.txt")
    //todo 2.将原始数据进行切分成一个一个的单词
    val words = lines.flatMap(
      line => {line.split(" ")}
    )
    //todo 3.对分词的结果进行分组操作（相同的单词放在一起）
    val wordgroup = words.groupBy(word => word)
    //todo 4.对分组之后的数据进行统计
    val wordcount = wordgroup.mapValues(
      v => {
        v.size
      }
    )
    wordcount
  }
}
