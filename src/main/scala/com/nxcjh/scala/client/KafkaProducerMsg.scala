package com.nxcjh.scala.client

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.io.Source
import scala.reflect.io.Path



/**
  * Created by wanzer on 2017/3/15.
  */
class KafkaProducerMsg(brokerList : String, topic : String) extends Runnable{

  private val BROKER_LIST = brokerList //"10.0.3.179:9092, 10.0.3.137:9092, 10.0.3.70:9092"
  private val TARGET_TOPIC = topic //"new"
  private val DIR = "e:\\data"

  /**
    * 1. 配置属性
    * metadata.broker.list : kafka集群的broker, 只需要指定3个即可
    * serializer.class : 如何序列化发送消息
    * request.required.acks : 1 代表需要broker接收到消息后acknowledgment, 默认是0
    * producer.type : 默认就是同步sync
    */
  private val props = new Properties()
  props.put("metadata.broker.list", this.BROKER_LIST)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("request.required.acks", "1")
  props.put("producer.type", "async")

  /**
    * 2. 创建Producer
    */
  private val config = new ProducerConfig(this.props)
  private val producer = new Producer[String, String](this.config)

  /**
    * 3. 产生并发送消息
    * 搜索目录dir下的所有包含transaction的文件并将每行的记录以消息的形式发送到kafka
    */
  def run() : Unit = {
    while(true){
      val files = Path(this.DIR).walkFilter(p => p.isFile && p.name.contains("transaction"))

      try{
        for (file <- files){
          println(file.name)

          val filename = file.toString()
          println(filename)
          val reader = Source.fromFile(filename)

          for (line <- reader.getLines()){
            println(line)
            val message = new KeyedMessage[String, String](this.TARGET_TOPIC, line)
            producer.send(message)
          }
          reader.close()
          //producer完成后, 将文件考到另一个目录, 之后delete
          val fileName = file.toFile.name
          file.toFile.copyTo(Path("e:\\tmp\\" + fileName + ".complate"))
          file.delete()
        }
      }catch {
        case e : Exception => println(e)
      }

      try{
        //sleep for 3 seconds after send a micro batch of message
        Thread.sleep(3000)
      }catch {
        case e : Exception => println(e)
      }
    }
  }
}

object KafkaProducerMsg{
  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("Usage: ProducerMsg master:9092, worker:9092 new")
    }

    new Thread(new KafkaProducerMsg(args(0), args(1))).start()
  }
}
