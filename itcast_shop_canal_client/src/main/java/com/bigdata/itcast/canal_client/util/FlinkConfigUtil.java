package com.bigdata.itcast.canal_client.util;

import com.bigdata.itcast.canal_client.config.MyConfig;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.net.URL;

/**
 * @ description: 使用flinkparam读取配置文件
 * @ author: spencer
 * @ date: 2020/12/9 17:38
 */
public class FlinkConfigUtil {

    public static void main(String[] args) throws IOException {

//        String canalServer = param.get("canal.server.ip");
//        System.out.println(canalServer);

        ParameterTool param = parameterTool();
        System.out.println(param.get(MyConfig.CANAL_SERVER_IP));
        System.out.println(param.get(MyConfig.KAFKA_TOPIC));
    }

    /**
     * 生成flink中的parameterTool
     * @return
     * @throws IOException
     */
    public static ParameterTool parameterTool() {
        URL url = FlinkConfigUtil.class.getClassLoader().getResource("config.properties");
        ParameterTool param = null;
        try {
            param = ParameterTool.fromPropertiesFile(url.getPath());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return param;
    }
}
