package cn.itcast.canal.bean;

import cn.itcast.canal.protobuf.CanalModel;
import cn.itcast.canal.protobuf.ProtoBufable;
import com.alibaba.fastjson.JSON;

import java.util.Map;

/**
 * @author yycstart
 * @create 2021-01-28 13:32
 *
 * 这个类是canal数据的protobuf的实现类
 * 能够使用protobuf序列化成bean对象
 * 目的：将binlog解析后的map对象，转换成protobuf序列化后的字节码数据，最终写入Kafka集群中
 *
 */
public class CanalRowData implements ProtoBufable {
    private String logfilename;
    private Long logfileoffset;
    private Long executeTime;
    private String schemaName;
    private String tableName;
    private String eventType;
    private Map<String, String> columns;


    /**
     * 定义构造方法，解析Map对象的binlogd
     * @return
     */
    public CanalRowData(Map map) {
        //解析Map对象所有参数
        if(map.size() > 0){
            this.logfilename = map.get("logfileName").toString();
            this.logfileoffset = Long.parseLong(map.get("logfileOffset").toString());
            this.executeTime = Long.parseLong(map.get("executeTime").toString());
            this.schemaName = map.get("schemaName").toString();
            this.tableName = map.get("tableName").toString();
            this.eventType = map.get("eventType").toString();
            this.columns = (Map<String, String>)map.get("columns");
        }
    }

    public String getLogfilename() {
        return logfilename;
    }

    public void setLogfilename(String logfilename) {
        this.logfilename = logfilename;
    }

    public Long getLogfileoffset() {
        return logfileoffset;
    }

    public void setLogfileoffset(Long logfileoffset) {
        this.logfileoffset = logfileoffset;
    }

    public Long getExecuteTime() {
        return executeTime;
    }

    public void setExecuteTime(Long executeTime) {
        this.executeTime = executeTime;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Map<String, String> getColumns() {
        return columns;
    }

    public void setColumns(Map<String, String> columns) {
        this.columns = columns;
    }


    /**
     * 学要将map对象解析出来的参数，赋值给protobuf对象，然后序列化后字节码返回
     * @return
     */
    @Override
    public byte[] toBytes() {
        CanalModel.RowData.Builder builder = CanalModel.RowData.newBuilder();
        builder.setLogfileName(this.getLogfilename());
        builder.setLogfileOffset(this.getLogfileoffset());
        builder.setExecuteTime(this.getExecuteTime());
        builder.setSchemaName(this.getSchemaName());
        builder.setTableName(this.getTableName());
        builder.setEventType(this.getEventType());

        for (String key : this.getColumns().keySet()) {
            builder.putColumns(key,this.getColumns().get(key));
        }

        //将传递的binlog数据解析后序列化成字节码数据返回
        return builder.build().toByteArray();
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
