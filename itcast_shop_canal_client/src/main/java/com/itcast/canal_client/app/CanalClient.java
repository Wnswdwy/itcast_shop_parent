package com.itcast.canal_client.app;

/**
 * @author yycstart
 * @create 2021-01-27 19:36
 */

import cn.itcast.canal.bean.CanalRowData;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.protobuf.InvalidProtocolBufferException;
import com.itcast.canal_client.kafka.KafkaSender;
import com.itcast.canal_client.util.ConfigUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Canal客户端程序
 * 与canal服务器建立连接，然后读取canalServer端binlog日志
 */
public class CanalClient {

    //定义canal客户端连接器
    private CanalConnector canalConnector;
    //设置每次拉取binlog日志的条数
    private  static final int BATCH_SIZE = 5*1024;
    //定义kafka的生产者工具类
    private KafkaSender kafkaSender;

    /**
     * 构造方法
     */
    public CanalClient(){
        //初始化客户端连接
        canalConnector = CanalConnectors.newClusterConnector(ConfigUtil.zookeeperServerIp(),
                ConfigUtil.canalServerDestination(),
                ConfigUtil.canalServerUsername(),
                ConfigUtil.canalServerPassword());

        //实例化Kafka的工具类
        kafkaSender = new KafkaSender();
    }

    /**
     * 开始执行
     */
    public void start(){

        //建立连接
        try {
            canalConnector.connect();
            //回滚上次的get请求，重新拉取数据
            canalConnector.rollback();
            //订阅匹配的数据库
            canalConnector.subscribe(ConfigUtil.canalSubscribeFilter());
            //不停的循环拉取binlog日志，
            while (true){
                //每次拉取5*1024条数据
                Message message = canalConnector.getWithoutAck(BATCH_SIZE);
                //获取batchId
                long batchId = message.getId();
                //获取binlog的条数
                int size = message.getEntries().size();
                if(size == 0 || size == -1){
                    //没有拉取到数据
                }else{
                //将binlog日志进行解析，解析后就是map对象
                    Map binlogMessageToMap = binlogMessageToMap(message);
                    //需要将map对象序列化成protobuf格式写入到Kafka中
                    CanalRowData rowData = new CanalRowData(binlogMessageToMap);
                    System.out.println(rowData);

                    if(binlogMessageToMap.size() > 0){
                        kafkaSender.send(rowData);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            canalConnector.disconnect();
        }

    }
    /**
     * 将binlog日志转换为Map结构
     * @param message
     * @return
     */
    private Map binlogMessageToMap(Message message) throws InvalidProtocolBufferException {
        Map rowDataMap = new HashMap();

        // 1. 遍历message中的所有binlog实体
        for (CanalEntry.Entry entry : message.getEntries()) {
            // 只处理事务型binlog
            if(entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN ||
                    entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            // 获取binlog文件名
            String logfileName = entry.getHeader().getLogfileName();
            // 获取logfile的偏移量
            long logfileOffset = entry.getHeader().getLogfileOffset();
            // 获取sql语句执行时间戳
            long executeTime = entry.getHeader().getExecuteTime();
            // 获取数据库名
            String schemaName = entry.getHeader().getSchemaName();
            // 获取表名
            String tableName = entry.getHeader().getTableName();
            // 获取事件类型 insert/update/delete
            String eventType = entry.getHeader().getEventType().toString().toLowerCase();

            rowDataMap.put("logfileName", logfileName);
            rowDataMap.put("logfileOffset", logfileOffset);
            rowDataMap.put("executeTime", executeTime);
            rowDataMap.put("schemaName", schemaName);
            rowDataMap.put("tableName", tableName);
            rowDataMap.put("eventType", eventType);

            // 获取所有行上的变更
            Map<String, String> columnDataMap = new HashMap<>();
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            List<CanalEntry.RowData> columnDataList = rowChange.getRowDatasList();
            for (CanalEntry.RowData rowData : columnDataList) {
                if(eventType.equals("insert") || eventType.equals("update")) {
                    for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                        columnDataMap.put(column.getName(), column.getValue().toString());
                    }
                }
                else if(eventType.equals("delete")) {
                    for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                        columnDataMap.put(column.getName(), column.getValue().toString());
                    }
                }
            }

            rowDataMap.put("columns", columnDataMap);
        }

        return rowDataMap;
    }
}
