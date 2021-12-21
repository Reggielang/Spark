package Case

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/*
HotCategoryTop10

 */
object Spark01_Case04 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)
    //todo 需求1：热门品类top10 --改版3


    val fileDatas: RDD[String] = sc.textFile("data/user_visit_action.txt")

    // 创建累加器对象
    val acc = new HotCategoryAccumulator()
    //注册累加器
    sc.register(acc,name="HotCategory")

    fileDatas.foreach(
      data=>{
        val datas: Array[String] = data.split("_")
        if (datas(6)!="-1"){
          //点击的场合
          acc.add((datas(6),"click"))
        }else if(datas(8)!="null"){
          //下单的场合
          val id = datas(8)
          val ids = id.split(",")
          ids.foreach(
            id=>{
              acc.add((id,"order"))
            }
          )
        }else if(datas(10)!="null"){
          //支付的场合
          val id = datas(8)
          val ids = id.split(",")
          ids.foreach(
            id=>{
              acc.add((id,"pay"))
            }
          )
        }
      }
    )

    //获得累加器的结果
    val resultMap: mutable.Map[String, HotCategoryCount] = acc.value

    val top10: List[HotCategoryCount] = resultMap.map(_._2).toList.sortWith(
      (left, right) => {
        if (left.clickCnt > right.clickCnt) {
          true
        } else if (left.clickCnt == right.clickCnt) {
          if (left.orderCnt > right.orderCnt) {
            true
          } else if (left.orderCnt == right.orderCnt) {
            left.payCnt > right.payCnt
          } else {
            false
          }
        } else {
          false
        }
      }
    ).take(10)



    //todo 读取文件，获取原始数据
    //todo 统计品类的点击数量

    top10.foreach(println)
    sc.stop()
  }
  case class HotCategoryCount(
                             cid:String,var clickCnt:Int,var orderCnt:Int,var payCnt:Int
                             )
  //自定义热门点击累加器
  //1.继承
  //2.定义泛型
  // IN:（品类ID，行为类型）
  // out:Map[品类ID，HotCategoryCount]
  //3.重写方法3+3
  //4.
  class HotCategoryAccumulator extends AccumulatorV2[(String,String),mutable.Map[String,HotCategoryCount]]{

    private val map = mutable.Map[String,HotCategoryCount]()


    override def isZero: Boolean = {
      map.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategoryCount]] = {
      new HotCategoryAccumulator()
    }

    override def reset(): Unit = {
      map.clear()
    }

    override def add(v: (String, String)): Unit = {
      val (cid, actionType) = v
      val hcc: HotCategoryCount = map.getOrElse(cid, HotCategoryCount(cid, 0, 0, 0))
      actionType match {
        case "click"=>hcc.clickCnt+=1
        case "order"=>hcc.orderCnt+=1
        case "pay"=>hcc.payCnt+=1
      }
      map.update(cid,hcc)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategoryCount]]): Unit = {
      other.value.foreach {
        case (cid, otherhcc) => {
          val thishcc: HotCategoryCount = map.getOrElse(cid, HotCategoryCount(cid, 0, 0, 0))
          thishcc.clickCnt += otherhcc.clickCnt
          thishcc.orderCnt += otherhcc.orderCnt
          thishcc.payCnt += otherhcc.payCnt
          map.update(cid, thishcc)
        }

      }
    }


    override def value: mutable.Map[String, HotCategoryCount] = {
      map
    }
  }
}
