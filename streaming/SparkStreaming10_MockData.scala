package streaming

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.util.Random


object SparkStreaming10_MockData {
  def main(args: Array[String]): Unit = {
    //生成模拟数据
    // 时间戳，省份，区域， 城市，用户，广告
    //* 格式 ：timestamp area city userid adid
    //app =》Kafka=》sparkstreaming =>分析

    // 创建配置对象
    val prop = new Properties()
    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](prop)

    while (true){

      mockdata().foreach(
        data=>{
          //向Kafka中生成数据
          val record = new ProducerRecord[String,String]("honglang",data)
          producer.send(record)
          println(data)
        }
      )
      Thread.sleep(2000)
    }

  }

  def mockdata()={
    val list = ListBuffer[String]()
    val arealist = ListBuffer[String]("华东","华北","华南")
    val citylist = ListBuffer[String]("北京","上海","成都")
    for(i<- 1 to 30){
      val area = arealist(new Random().nextInt(3))
      val city = citylist(new Random().nextInt(3))
      var userid =new Random().nextInt(6)+1
      var adid =new Random().nextInt(6)+1
      list.append(s"${System.currentTimeMillis()} ${area} ${city} ${userid} ${adid}")

    }
    list
  }
  
}


