package cn.itcast.shop.realtime.etl.utils

import cn.itcast.canal.bean.CanalRowData
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema

/**
 * @author yycstart
 * @create 2021-01-29 15:04
 *
 *   自定义反序列化类，继承AbstractDeserializationSchema抽象类
 *   参考：SimpleStringSchema
 */
class CanalRowDataDeserializerSchema extends AbstractDeserializationSchema[CanalRowData]{
  override def deserialize(message: Array[Byte]): CanalRowData = {
    new CanalRowData(message)
  }
}
