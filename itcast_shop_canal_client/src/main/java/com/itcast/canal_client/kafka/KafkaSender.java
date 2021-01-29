package com.itcast.canal_client.kafka;





import cn.itcast.canal.bean.CanalRowData;
import cn.itcast.canal.protobuf.CanalModel;
import com.itcast.canal_client.util.ConfigUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author yycstart
 * @create 2021-01-28 13:58
 *
 * Kafka生产者工具类
 */
public class KafkaSender {
    //定义properties，封装Kafka相关参数
    private Properties kafkaPros = new Properties();
    //定义生产者对象，value使用的是自定义序列化的方式，该序列化的方式要求传递到的是一个protobuf的子类
    private KafkaProducer<String, CanalRowData> kafkaProducer;

    //初始化Kafka的生产者对象
    public KafkaSender(){
        kafkaPros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigUtil.kafkaBootstrap_servers_config());
        kafkaPros.put(ProducerConfig.BATCH_SIZE_CONFIG,ConfigUtil.kafkaBatch_size_config());
        kafkaPros.put(ProducerConfig.ACKS_CONFIG,ConfigUtil.kafkaAcks());
        kafkaPros.put(ProducerConfig.RETRIES_CONFIG,ConfigUtil.kafkaRetries());
        kafkaPros.put(ProducerConfig.CLIENT_ID_CONFIG,ConfigUtil.kafkaClient_id_config());
        kafkaPros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,ConfigUtil.kafkaKey_serializer_class_config());
        kafkaPros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,ConfigUtil.kafkaValue_serializer_class_config());

        //实例化生产者对象
        kafkaProducer = new KafkaProducer<>(kafkaPros);
    }

    /**
     * 传递参数，将数据写入Kafka集群
     * @param rowData
     */

    public void send(CanalRowData rowData){
        kafkaProducer.send(new ProducerRecord<>(ConfigUtil.kafkaTopic(), rowData));
    }
}
