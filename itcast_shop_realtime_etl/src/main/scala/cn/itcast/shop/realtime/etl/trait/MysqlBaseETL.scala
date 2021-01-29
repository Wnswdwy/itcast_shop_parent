package cn.itcast.shop.realtime.etl.`trait`

import cn.itcast.canal.bean.CanalRowData
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @author yycstart
 * @create 2021-01-28 17:43
 *        根据数据的来源不同，可以抽象出两个抽象类
 *        给类主要消费CanalTopic中的数据，需要将消费到的字节码数据进行反序列化（自定义反序列化）成CanalRowData
 */
abstract class MysqlBaseETL(env:StreamExecutionEnvironment)  extends BaseETL[CanalRowData]{
  /**
   * 根据业务可以抽取出来Kafka的读取方法，因为所有的ETL都会操作Kafka
   *
   * @param topic
   * @return
   */
  override def getKafkaDataStream(topic: String): DataStream[CanalRowData] = ???

  /**
   * 根据业务可以抽取出操作方法，因为所有ETL都会有擦作方法
   */
  override def process(topic: String): Unit = ???
}
