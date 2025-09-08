package com.example.modbus;

import com.ghgande.j2mod.modbus.io.ModbusTCPTransaction;
import com.ghgande.j2mod.modbus.msg.*;
import com.ghgande.j2mod.modbus.net.TCPMasterConnection;
import com.ghgande.j2mod.modbus.util.ModbusUtil;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ModbusMqttClient {

    // MQTT配置类
    static class MqttConfig {
        public String broker;
        public String username;
        public String password;
        public String clientId;
        public String topic;
    }

    // 点位配置类
    static class PointConfig {
        public String serverIp;
        public int serverPort;
        public int dataAcqInterval;
        public List<Point> point;
    }

    // 点位类
    static class Point {
        public String name;
        public String deviceAddr;
        public String functionCode;
        public String startRegister;
        public String registerCount;
        public String dataType;
    }

    // 数据点结果类
    static class DataPoint {
        public String name;
        public String pv;
        public String updateTime;

        public DataPoint(String name, String pv, String updateTime) {
            this.name = name;
            this.pv = pv;
            this.updateTime = updateTime;
        }
    }

    private final Gson gson = new Gson();
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private MqttClient mqttClient;
    private TCPMasterConnection modbusConnection;
    private ModbusTCPTransaction transaction;

    public void run() {
        MqttConfig mqttConfig = null;
        PointConfig pointConfig = null;
        try {
            mqttConfig = loadMqttConfig("config/mqtt-config.json");
            pointConfig = loadPointConfig("config/point.json");
            mqttConfig.clientId = "mqtt_snd_plc_pub" + UUID.randomUUID().toString();
        } catch (Exception e){
            log.error("loadMqttConfig fail:", e);
            e.printStackTrace();
            return;
        }

        while(true){
            try {
                mqttClient = connectMqtt(mqttConfig);
                break;
            } catch (Exception e) {
                log.error("connectMqtt fail: ", e);
                try{
                    Thread.sleep(5000);
                } catch (Exception ex) {
                    log.error("sleep fail:", ex);
                }
            }
        }

        while(true) {
            // 初始化连接
            try {
                modbusConnection = connectModbus(pointConfig);
                transaction = new ModbusTCPTransaction(modbusConnection);
                break;
            } catch (Exception e) {
                log.error("Modebus connect fail:", e);
                try{
                    Thread.sleep(5000);
                } catch (Exception ex) {
                    log.error("sleep fail:", ex);
                }
            }
        }

        // 将配置保存为final变量，供lambda表达式使用
        final MqttConfig finalMqttConfig = mqttConfig;
        final PointConfig finalPointConfig = pointConfig;

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        // 每隔dataAcqInterval秒执行一次数据读取和发布
        scheduler.scheduleAtFixedRate(() -> {
            try {
                log.info("===============================================");
                log.info("开始执行数据采集任务: " + new Date());
                log.info("===============================================");

                // 检查并确保连接有效
                ensureConnections(finalMqttConfig, finalPointConfig);
                readAndPublishData(finalPointConfig, mqttClient, finalMqttConfig.topic);
                log.info("数据采集任务完成\n");
            } catch (Exception e) {
                log.error("数据采集任务执行失败: ", e);
                e.printStackTrace();
            }
        }, 0, pointConfig.dataAcqInterval, TimeUnit.SECONDS); // 立即开始，每dataAcqInterval秒执行一次

        // 添加关闭钩子以优雅关闭
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
            }

            // 关闭连接
            try {
                if (mqttClient != null && mqttClient.isConnected()) {
                    mqttClient.disconnect();
                    mqttClient.close();
                }
                if (modbusConnection != null) {
                    modbusConnection.close();
                }
            } catch (Exception e) {
                log.error("关闭连接时出错: ", e);
            }

            log.info("程序已关闭");
        }));
    }

    // 读取MQTT配置
    private MqttConfig loadMqttConfig(String configFile) throws IOException {
        try (InputStreamReader reader = new InputStreamReader(new FileInputStream(configFile), StandardCharsets.UTF_8)) {
            return gson.fromJson(reader, MqttConfig.class);
        }
    }

    // 读取点位配置
    private PointConfig loadPointConfig(String configFile) throws IOException {
        try (InputStreamReader reader = new InputStreamReader(new FileInputStream(configFile), StandardCharsets.UTF_8)) {
            return gson.fromJson(reader, PointConfig.class);
        }
    }

    // 连接Modbus
    private TCPMasterConnection connectModbus(PointConfig config) throws Exception {
        TCPMasterConnection connection = null;
        try {
            InetAddress address = InetAddress.getByName(config.serverIp);
            connection = new TCPMasterConnection(address);
            connection.setPort(config.serverPort);
            connection.connect();
            connection.setTimeout(5000); // 设置5秒超时
            log.info("已连接到Modbus TCP服务器: " + config.serverIp + ":" + config.serverPort);
        } catch (Exception e) {
            log.error("连接Modbus TCP服务器失败: ", e);
            if(connection != null){
                connection.close();
                log.info("已关闭Modbus连接");
            }
            throw e;
        }
        return connection;
    }

    // 确保连接有效
    private void ensureConnections(MqttConfig mqttConfig, PointConfig pointConfig) {
        // 检查MQTT连接
        try {
            if (mqttClient == null) {
                log.warn("MQTT客户端为空，重新创建连接...");
                mqttClient = connectMqtt(mqttConfig);
            }
        } catch (Exception e) {
            log.error("MQTT重连失败: ", e);
        }

        // 检查Modbus连接
        try {
            if (modbusConnection == null || !modbusConnection.isConnected()) {
                log.warn("Modbus连接断开，尝试重新连接...");
                if (modbusConnection != null) {
                    modbusConnection.close();
                }
                modbusConnection = connectModbus(pointConfig);
                transaction.setConnection(modbusConnection);
            }
        } catch (Exception e) {
            log.error("Modbus重连失败: ", e);
        }
    }

    // 连接MQTT Broker
    private MqttClient connectMqtt(MqttConfig config) throws Exception {
        MqttClient client = null;
        try {
            client = new MqttClient(config.broker, config.clientId, new MemoryPersistence());
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
            options.setAutomaticReconnect(true); // 启用自动重连
            options.setConnectionTimeout(15); // 15秒连接超时
            options.setKeepAliveInterval(30); // 30秒心跳间隔

            if (!config.username.isEmpty() && !config.password.isEmpty()) {
                options.setUserName(config.username);
                options.setPassword(config.password.toCharArray());
            }

            client.connect(options);
            log.info("已连接到MQTT Broker: " + config.broker);
        } catch (Exception e) {
            log.error("MQTT连接失败: ", e);
            if(client != null){
                client.close();
                log.info("已关闭MQTT连接");
            }
            throw e;
        }
        return client;
    }

    // 读取Modbus数据并发布到MQTT
    private void readAndPublishData(PointConfig config, MqttClient mqttClient, String topic) throws Exception {
        try {
            List<DataPoint> dataPoints = new ArrayList<>();
            String updateTime = dateFormat.format(new Date());

            // 遍历所有点位
            for (Point point : config.point) {
                try {
                    int functionCode = Integer.parseInt(point.functionCode);
                    int startReg = Integer.parseInt(point.startRegister);
                    int regCount = Integer.parseInt(point.registerCount);
                    int unitId = Integer.parseInt(point.deviceAddr);

                    log.info("\n--- 读取点位: " + point.name + " ---");
                    String value = readModbusValue(transaction, functionCode, startReg, regCount, unitId, point.dataType);
                    dataPoints.add(new DataPoint(point.name, value, updateTime));
                    log.info("读取结果: " + point.name + " = " + value);
                } catch (Exception e) {
                    log.error("读取点位 " + point.name + " 失败: ", e);
                    dataPoints.add(new DataPoint(point.name, "ERROR", updateTime));
                }
            }

            // 发布到MQTT
            String jsonData = gson.toJson(dataPoints);
            MqttMessage message = new MqttMessage(jsonData.getBytes(StandardCharsets.UTF_8));
            message.setQos(0);
            mqttClient.publish(topic, message);
            log.info("\n已发布数据到MQTT主题: " + topic);
            log.info("发布数据: " + jsonData);

        } catch (Exception e)  {
            log.error("readAndPublishData error:", e);
        }
    }

    // 读取Modbus值
    private String readModbusValue(ModbusTCPTransaction transaction, int functionCode,
                                          int startReg, int regCount, int unitId, String dataType) throws Exception {
        switch (functionCode) {
            case 1: // 读线圈状态
                ReadCoilsRequest coilRequest = new ReadCoilsRequest();
                coilRequest.setReference(startReg);
                coilRequest.setBitCount(regCount);
                coilRequest.setUnitID(unitId);

                // 打印发送报文
                printModbusRequest(coilRequest, "读线圈状态");

                transaction.setRequest(coilRequest);
                transaction.execute();

                ReadCoilsResponse coilResponse = (ReadCoilsResponse) transaction.getResponse();

                // 打印接收报文
                printModbusResponse(coilResponse, "读线圈状态响应");

                StringBuilder coilResult = new StringBuilder();
                int realCount = Math.min(regCount, coilResponse.getBitCount());
                for (int i = 0; i < realCount; i++) {
                    coilResult.append(coilResponse.getCoilStatus(i) ? "1" : "0");
                    if (i < regCount - 1) coilResult.append(",");
                }

                // 根据dataType处理返回值
                if ("bool".equals(dataType) && realCount == 1) {
                    return coilResponse.getCoilStatus(0) ? "1" : "0";
                }

                return coilResult.toString();

            case 3: // 读保持寄存器
                ReadMultipleRegistersRequest regRequest = new ReadMultipleRegistersRequest();
                regRequest.setReference(startReg);
                regRequest.setWordCount(regCount);
                regRequest.setUnitID(unitId);

                // 打印发送报文
                printModbusRequest(regRequest, "读保持寄存器");

                transaction.setRequest(regRequest);
                transaction.execute();

                ReadMultipleRegistersResponse regResponse = (ReadMultipleRegistersResponse) transaction.getResponse();

                // 打印接收报文
                printModbusResponse(regResponse, "读保持寄存器响应");

                // 根据数据类型处理返回值
                if ("float".equals(dataType) && regCount == 2) {
                    // 处理float类型，大端模式
                    int highWord = regResponse.getRegisterValue(0);
                    int lowWord = regResponse.getRegisterValue(1);
                    int intValue = (highWord << 16) | lowWord;
                    float floatValue = Float.intBitsToFloat(intValue);
                    return String.format("%.6f", floatValue);
                } else {
                    // 默认处理方式
                    StringBuilder regResult = new StringBuilder();
                    for (int i = 0; i < Math.min(regCount, regResponse.getWordCount()); i++) {
                        regResult.append(regResponse.getRegisterValue(i));
                        if (i < regCount - 1) regResult.append(",");
                    }
                    return regResult.toString();
                }

            default:
                return "UNSUPPORTED_FUNCTION_CODE";
        }
    }

    // 打印Modbus请求报文
    private void printModbusRequest(ModbusRequest request, String description) {
        try {
            byte[] requestBytes = request.getMessage();
            int transactionId = request.getTransactionID();
            int protocolId = request.getProtocolID();
            int unitId = request.getUnitID();
            int functionCode = request.getFunctionCode();

            // 构造完整的请求报文
            byte[] fullRequest = new byte[requestBytes.length + 8];

            // 添加MBAP头部
            fullRequest[0] = (byte) (transactionId >> 8);
            fullRequest[1] = (byte) (transactionId & 0xFF);
            fullRequest[2] = (byte) (protocolId >> 8);
            fullRequest[3] = (byte) (protocolId & 0xFF);
            fullRequest[4] = (byte) ((requestBytes.length + 2) >> 8);
            fullRequest[5] = (byte) ((requestBytes.length + 2) & 0xFF);
            fullRequest[6] = (byte) unitId;

            // 添加PDU
            fullRequest[7] = (byte) functionCode;
            System.arraycopy(requestBytes, 0, fullRequest, 8, requestBytes.length);

            log.info("发送报文 (" + description + "): " + bytesToHex(fullRequest));
        } catch (Exception e) {
            log.error("打印请求报文失败: ", e);
        }
    }

    // 打印Modbus响应报文
    private void printModbusResponse(ModbusResponse response, String description) {
        try {
            byte[] responseBytes = response.getMessage();
            int transactionId = response.getTransactionID();
            int protocolId = response.getProtocolID();
            int unitId = response.getUnitID();
            int functionCode = response.getFunctionCode();

            // 构造完整的响应报文
            byte[] fullResponse = new byte[responseBytes.length + 8];

            // 添加MBAP头部
            fullResponse[0] = (byte) (transactionId >> 8);
            fullResponse[1] = (byte) (transactionId & 0xFF);
            fullResponse[2] = (byte) (protocolId >> 8);
            fullResponse[3] = (byte) (protocolId & 0xFF);
            fullResponse[4] = (byte) ((responseBytes.length + 2) >> 8);
            fullResponse[5] = (byte) ((responseBytes.length + 2) & 0xFF);
            fullResponse[6] = (byte) unitId;

            // 添加PDU
            fullResponse[7] = (byte) functionCode;
            System.arraycopy(responseBytes, 0, fullResponse, 8, responseBytes.length);

            log.info("接收报文 (" + description + "): " + bytesToHex(fullResponse));
        } catch (Exception e) {
            log.error("打印响应报文失败: ", e);
        }
    }

    // 将字节数组转换为十六进制字符串
    private String bytesToHex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            result.append(String.format("%02X", bytes[i]));
            if (i < bytes.length - 1) {
                result.append(" ");
            }
        }
        return result.toString();
    }
}

