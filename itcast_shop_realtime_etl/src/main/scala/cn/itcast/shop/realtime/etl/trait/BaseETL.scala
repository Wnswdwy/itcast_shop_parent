package cn.itcast.shop.realtime.etl.`trait`

import cn.itcast.shop.realtime.etl.utils.KafkaProps
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper

/**
 * @author yycstart
 * @create 2021-01-28 17:35
 *
 *        定义特质，抽取所有etl操作的公共方法
 */
trait BaseETL[T]{


  /**
   * 定义方法，将数据写入kafka
   * 创建Kafka的生产者对象
   */
  def kafkaProducer(topic:String) = {
    //将所有ETL后的数据写入到Kafka集群，写入时候都是Json格式
    new FlinkKafkaProducer011[String](
      topic,
      //这种方式常用于读取kafka中的数据，ETL后重新写入Kafka集群
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema())
      ,KafkaProps.getKafkaProperties()
    )
  }



  /**
   * 根据业务可以抽取出来Kafka的读取方法，因为所有的ETL都会操作Kafka
   * @param topic
   * @return
   */
  def getKafkaDataStream(topic:String) :DataStream[T]

  /**
   * 根据业务可以抽取出操作方法，因为所有ETL都会有擦作方法
   */
  def process()
}
