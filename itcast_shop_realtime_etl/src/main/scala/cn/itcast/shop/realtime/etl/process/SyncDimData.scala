package cn.itcast.shop.realtime.etl.process


import cn.itcast.canal.bean.CanalRowData
import cn.itcast.shop.realtime.etl.`trait`.MysqlBaseETL
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @author yycstart
 * @create 2021-01-29 18:24
 *        增量数据更新维度数据到Redis中
 */
case class SyncDimData(env: StreamExecutionEnvironment) extends MysqlBaseETL(env){
  /**
   * 根据业务可以抽取出操作方法，因为所有ETL都会有擦作方法
   */
  override def process(topic: String): Unit = {
    /**
     * 实现步骤：
     *    1. 获取数据源
     *    2. 过滤出来维度表
     *    3. 处理同步过来的数据，更新到Redis
     */

    //1. 获取数据源
    val canalDataStream: DataStream[CanalRowData] = getKafkaDataStream()
    //2. 过滤出来的维度表
    val dimRowDataStream: DataStream[CanalRowData] = canalDataStream.filter(
      rowData => {
        rowData.getTableName match {
          case "itcast_goods" => true
          case "itcast_shops" => true
          case "itcast_goods_cats" => true
          case "itcast_org" => true
          case "itcast_shop_cats" => true
          //一定要加上else，否则会抛出异常
          case _ => false
        }
      }
    )
    //3. 处理同步过来的数据，更新到Redis
//      dimRowDataStream.addSink()
  }
}
