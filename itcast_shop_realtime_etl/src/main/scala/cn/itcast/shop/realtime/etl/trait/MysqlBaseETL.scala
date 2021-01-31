package cn.itcast.shop.realtime.etl.`trait`

import cn.itcast.canal.bean.CanalRowData
import cn.itcast.shop.realtime.etl.utils.{CanalRowDataDeserializerSchema, GlobalConfigUtil, KafkaProps}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.api.scala._

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
  override def getKafkaDataStream(topic: String = GlobalConfigUtil.`input.topic.canal`): DataStream[CanalRowData] = {
    //消费的是Kafka的canal数据，而binlog日志进行protobuf序列化，所以读取到的数据需要进行反序列化
    val canalKafkaConsumer = new FlinkKafkaConsumer011[CanalRowData](
      topic,
      //自定义的反序列化对象，可以解析Kafka写入到protobuf格式的数据
      new CanalRowDataDeserializerSchema(),
      KafkaProps.getKafkaProperties()
    )

    //将消费者实例实例到环境中
    val canalDataStream: DataStream[CanalRowData] = env.addSource(canalKafkaConsumer)
    //返回消费到的数据
    canalDataStream
  }
}
