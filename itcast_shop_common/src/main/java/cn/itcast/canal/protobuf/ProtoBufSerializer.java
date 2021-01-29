package cn.itcast.canal.protobuf;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * @author yycstart
 * @create 2021-01-28 13:11
 */

/**
 * 实现KafkaValue的自定义序列化对象
 * 要求传递的泛型必须继承自protobuf接口的实现类，才可以被序列化成功
 */
public class ProtoBufSerializer implements Serializer<ProtoBufable> {
    @Override
    public void configure(Map<String, ?> map, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, ProtoBufable data) {
        return data.toBytes();
    }

    @Override
    public void close() {

    }
}
