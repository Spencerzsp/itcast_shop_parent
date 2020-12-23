package com.itcast.canal.bean;

import com.alibaba.fastjson.JSON;
import com.bigdata.canal.protobuf.CanalModel;
import com.google.protobuf.InvalidProtocolBufferException;
import com.itcast.canal.protobuf.ProtoBufable;

import java.util.HashMap;
import java.util.Map;

/**
 * CanalRowData->Protobuf->ProtobufSerializer->Serializer(Protobuf)
 * @ description: binglog日志的protobuf的实现类
 * 能够使用protobuf序列化成bean的对象
 * 目的：将binglog解析后的map对象，转换成protobuf序列化后的字节码数据，最终写入到kafka集群
 * @ author: spencer
 * @ date: 2020/12/9 16:19
 */
public class CanalRowData implements ProtoBufable {

    private String logfileName;
    private Long logfileOffset;
    private Long executeTime;
    private String schemaName;
    private String tableName;
    private String eventType;

    private Map<String, String> columns;

    public String getLogfileName() {
        return logfileName;
    }

    public void setLogfileName(String logfileName) {
        this.logfileName = logfileName;
    }

    public Long getLogfileOffset() {
        return logfileOffset;
    }

    public void setLogfileOffset(Long logfileOffset) {
        this.logfileOffset = logfileOffset;
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
     * 定义构造方法，将map对象传入
     */
    public CanalRowData(Map map) {

        if (map.size() > 0) {
            this.logfileName = map.get("logfileName").toString();
            this.logfileOffset = Long.parseLong(map.get("logfileOffset").toString());
            this.executeTime = Long.parseLong(map.get("executeTime").toString());
            this.schemaName = map.get("schemaName").toString();
            this.tableName = map.get("tableName").toString();
            this.eventType = map.get("eventType").toString();
            this.columns = (Map<String, String>) map.get("columns");
        }
    }

    /**
     * 将字节码数据转换成CanalRowData对象返回
     * @param bytes
     */
    public CanalRowData(byte[] bytes){
        try {
            CanalModel.RowData rowData = CanalModel.RowData.parseFrom(bytes);
            this.logfileName = rowData.getLogfileName();
            this.logfileOffset = rowData.getLogfileOffset();
            this.executeTime = rowData.getExecuteTime();
            this.schemaName = rowData.getSchema();
            this.tableName = rowData.getTableName();
            this.eventType = rowData.getEventType();

            this.columns = new HashMap<String, String>();
            this.columns.putAll(rowData.getColumnsMap());

        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    /**
     * 需要将map对象解析出来的参数，赋值给protobuf对象，然后返回二进制字节码数据
     * @return
     */

    @Override
    public byte[] toBytes() {
        CanalModel.RowData.Builder rowDataBuilder = CanalModel.RowData.newBuilder();
        rowDataBuilder.setLogfileName(this.logfileName);
        rowDataBuilder.setLogfileOffset(this.logfileOffset);
        rowDataBuilder.setExecuteTime(this.executeTime);
        rowDataBuilder.setSchema(this.schemaName);
        rowDataBuilder.setTableName(this.tableName);
        rowDataBuilder.setEventType(this.eventType);
        rowDataBuilder.putAllColumns(this.columns);

        // 将序列化后的字节码数据返回
        return rowDataBuilder.build().toByteArray();
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
