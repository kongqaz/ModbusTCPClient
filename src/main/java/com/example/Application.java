package com.example;

import com.example.modbus.ModbusMqttClient;

public class Application {
    public static void main(String[] args) {
        ModbusMqttClient modbusMqttClient = new ModbusMqttClient();
        modbusMqttClient.run();
    }
}
