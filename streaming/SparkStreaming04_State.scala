package streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreaming04_State {
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

    val datas: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    val wordToOne: DStream[(String, Int)] = datas.map((_, 1))

    //val wordCount: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)

    //updatestatebykey 根据key对数据的状态进行更新
    // 传递的参数中含有两个值，1.表示相同key的value数据
    //2.表示缓存区相同key的value数据
    val state: DStream[(String, Int)] = wordToOne.updateStateByKey(
      (seq: Seq[Int], buff: Option[Int]) => {
        val newCount = buff.getOrElse(0) + seq.sum
        Option(newCount)
      }
    )



    state.print()

    ssc.start()

    ssc.awaitTermination()
  }
}


