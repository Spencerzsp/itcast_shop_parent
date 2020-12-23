package com.bigdata.itcast.canal_client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.bigdata.itcast.canal_client.config.MyConfig;
import com.bigdata.itcast.canal_client.kafka.KafkaSender;
import com.bigdata.itcast.canal_client.util.ConfigUtil;
import com.bigdata.itcast.canal_client.util.FlinkConfigUtil;
import com.google.protobuf.InvalidProtocolBufferException;
import com.itcast.canal.bean.CanalRowData;

import java.net.InetSocketAddress;
import java.util.*;

/**
 * @ description: canal客户端与服务端建立连接，获取服务端binglog日志
 *                解析binglog日志为protobuf格式的对象，转换为二进制数据写入kafka
 * @ author: spencer
 * @ date: 2020/12/9 15:10
 */
public class CanalClient {

    // 每次拉取binglog日志的数量
    private static final int BATCH_SIZE = 5 * 1024;
    // canal客户端连接器
    CanalConnector canalConnector = null;

    String[] canalServers = null;

    List<InetSocketAddress> canalAdrressList = null;

    // kafka生产者工具类
    KafkaSender kafkaSender;

    /**
     * 构造方法初始化
     */
    public CanalClient() {
//        canalConnector = CanalConnectors.newClusterConnector(
//                ConfigUtil.zookeeperServerIp(),
//                ConfigUtil.canaServerDestination(),
//                ConfigUtil.canaServerUsername(),
//                ConfigUtil.canaServerPassword()
//        );
        // 获取canal集群地址
//        canalServers = ConfigUtil.canaServerIp().split(",");
        canalServers = FlinkConfigUtil.parameterTool().get(MyConfig.CANAL_SERVER_IP).split(",");
        canalAdrressList = new ArrayList<>();
        for (String canalServer : canalServers) {
            InetSocketAddress address = new InetSocketAddress(canalServer, Integer.parseInt(ConfigUtil.canalServerPort()));
            canalAdrressList.add(address);
        }
        canalConnector = CanalConnectors.newClusterConnector(
                canalAdrressList,
//                ConfigUtil.canaServerDestination(),
//                ConfigUtil.canaServerUsername(),
//                ConfigUtil.canaServerPassword()
                FlinkConfigUtil.parameterTool().get(MyConfig.DESTINATION),
                FlinkConfigUtil.parameterTool().get(MyConfig.CANAL_SERVER_USERNAME),
                FlinkConfigUtil.parameterTool().get(MyConfig.CANAL_SERVER_PASSWORD)

        );
        kafkaSender = new KafkaSender();
    }

    /**
     * 启动方法
     */
    public void start() {

        try {
            // 建立连接
            canalConnector.connect();
            // 回滚
            canalConnector.rollback();
            // 订阅主题
            canalConnector.subscribe(FlinkConfigUtil.parameterTool().get(MyConfig.CANAL_SUBSCRIBE_FILTER));
            // 循环拉取binglog日志数据
            while (true) {
                Message message = canalConnector.getWithoutAck(BATCH_SIZE);
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (size == 0 || size == -1) {
                    // 没有拉取到数据
                } else {
                    // 拉取到了数据
                    Map<String, Object> binglogMessageToMap = binglogToMap(message);
//                    System.out.println(binglogMessageToMap);

                    // 将map对象序列化成protobuf格式写入到kafka中
                    CanalRowData canalRowData = new CanalRowData(binglogMessageToMap);
                    System.out.println(canalRowData);
                    // 将对象转换成protobuf格式的二进制数据
//                    byte[] bytes = canalRowData.toBytes();
                    if (binglogMessageToMap.size() > 0){
                        kafkaSender.send(canalRowData);
                    }
                }
                // 提交偏移量
                canalConnector.ack(batchId);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            canalConnector.disconnect();
        }
    }

    /**
     * 将binglog日志解析为map
     *
     * @param message
     * @return
     */
    private Map<String, Object> binglogToMap(Message message) throws InvalidProtocolBufferException {
        // 创建存放数据的map
        Map<String, Object> rowDataMap = new HashMap<>();
        for (CanalEntry.Entry entry : message.getEntries()) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN ||
                    entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }
            String logfileName = entry.getHeader().getLogfileName();
            long logfileOffset = entry.getHeader().getLogfileOffset();
            long executeTime = entry.getHeader().getExecuteTime();
            String schemaName = entry.getHeader().getSchemaName();
            String tableName = entry.getHeader().getTableName();
            String eventType = entry.getHeader().getEventType().toString().toLowerCase();

            rowDataMap.put("logfileName", logfileName);
            rowDataMap.put("logfileOffset", logfileOffset);
            rowDataMap.put("executeTime", executeTime);
            rowDataMap.put("schemaName", schemaName);
            rowDataMap.put("tableName", tableName);
            rowDataMap.put("eventType", eventType);

            // 获取行上数据的所有变更
            Map<String, String> columnDataMap = new HashMap<>();
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
            for (CanalEntry.RowData rowData : rowDatasList) {
                if (eventType.equals("insert") || eventType.equals("update")){
                    for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                        columnDataMap.put(column.getName(), column.getValue());
                    }
                } else if (eventType.equals("delete")){
                    for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                        columnDataMap.put(column.getName(), column.getValue());
                    }
                }
            }
            rowDataMap.put("columns", columnDataMap);
        }
        return rowDataMap;
    }
}
