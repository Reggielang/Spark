package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreaming05_Transform {
  def main(args: Array[String]): Unit = {
    //todo 创建环境对象
    // StreamingContext需要2个参数：第一个参数表示环境配置
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数表示批量处理的周期(采集周期)
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //使用有状态操作时，需要设定 检查点路径
    ssc.checkpoint("cp")

    //无状态数据操作，只对采集周期内的数据进行处理
    // 在某些场合下，需要保留数据统计结果，实现数据的汇总

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    //transform方法将底层的rdd获取到后进行操作
    // DStream功能不完善的时候
    // 需要代码周期性执行
    //codes:Driver端
    val newDS: DStream[String] = lines.transform(
      rdd => {
        //codes:Driver端（周期性的执行）
        rdd.map(
          str=>{
            //codes:Executor端
            str
          }
        )
      }
    )
    //codes: Driver端
    val newDS1: DStream[String] = lines.map(
      data => {
        //codes:Executor端
        data
      }
    )

    ssc.start()

    ssc.awaitTermination()
  }
}


