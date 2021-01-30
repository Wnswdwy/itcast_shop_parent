package cn.itcast.shop.realtime.etl.`trait`

import org.apache.flink.streaming.api.scala.DataStream

/**
 * @author yycstart
 * @create 2021-01-28 17:35
 *
 *        定义特质，抽取所有etl操作的公共方法
 */
trait BaseETL[T]{

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
