package cn.itcast.shop.realtime.etl.process


import java.util.concurrent.TimeUnit

import cn.itcast.canal.bean.CanalRowData
import cn.itcast.shop.realtime.etl.`trait`.MysqlBaseETL
import cn.itcast.shop.realtime.etl.async.AsyncOrderDetailRedisRequest
import cn.itcast.shop.realtime.etl.bean.OrderGoodsWideEntity
import cn.itcast.shop.realtime.etl.utils.{GlobalConfigUtil, HbaseUtil}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

/**
 * @author yycstart
 * @create 2021-02-01 17:25
 *        订单明细实时ETL
 *        1. 将订单明细数据的事实表和维度表的数据关联后写入到HBase中
 *        2. 将拉宽后的订单数据保存到Kafka中，供druid实时摄取
 *
 *
 *        数据为什么要存两份？
 *        1）保存到HBase的数据是明细数据，可以持久化的存储，将来需要分析明细数据时候，可以查询
 *        2）保存到Kafka的数据，是有时效性的，会根据Kafka设置的保存策略过期删除数据，摄取到druid以后就不是明细数据了
 *          已经对明细数据进行了预聚合操作
 *
 *          Druid = KyLin + HBase
 */
case class orderDetailDataETL(env: StreamExecutionEnvironment) extends MysqlBaseETL(env){
  /**
   * 根据业务可以抽取出操作方法，因为所有ETL都会有擦作方法
   */



  override def process(): Unit = {
    /**
     * 实现步骤：
     *  1. 获取canal中的订单明细数据，过滤出订单明细数据，将CanalRowData转换成orderGoods样例类
     *  2. 将订单明细表的数据进行拉宽操作
     *  3. 将拉宽后的订单明细表的数据转换成json格式写入到Kafka集群，供druid进行实时的摄取
     *  4. 将拉宽后的订单明细表的数据写入到HBase中，供后续订单明细详细数据的查询
     */

    //1. 获取canal中的订单明细数据，过滤出订单明细数据，将CanalRowData转换成orderGoods样例类
    val orderGoodsCanalDataStream: DataStream[CanalRowData] = getKafkaDataStream().filter(_.getTableName == "itcast_order_goods")
    //2. 将订单明细表的数据进行拉宽操作
    /**
     * unorderedWait方法的参数解释：
     *      orderGoodsCanalDataStream      =>要关联的数据源
     *      AsyncOrderDetailRedisRequest   =>异步请求的对象
     *      1                              =>超时时间
     *      TimeUnit.SECONDS               =>超时的时间单位
     *      100                            =>异步io的最大并发数
     *
     *
     */
    val orderGoodsWideEntityDataStream: DataStream[OrderGoodsWideEntity] = AsyncDataStream.unorderedWait(orderGoodsCanalDataStream, new AsyncOrderDetailRedisRequest, 1, TimeUnit.SECONDS, 100)
    orderGoodsWideEntityDataStream.printToErr("拉宽后的订单明细数据>>")

    //3. 将拉宽后的订单明细表的数据转换成json格式写入到Kafka集群，供druid进行实时的摄取
    val orderGoodsWideJsonDataStream: DataStream[String] = orderGoodsWideEntityDataStream.map(orderGoods => {
      JSON.toJSONString(orderGoods, SerializerFeature.DisableCircularReferenceDetect)
    })
    //4. 将拉宽后的订单明细表的数据写入到HBase中，供后续订单明细详细数据的查询
    orderGoodsWideJsonDataStream.addSink(kafkaProducer(GlobalConfigUtil.`output.topic.order_detail`))
  }
}
