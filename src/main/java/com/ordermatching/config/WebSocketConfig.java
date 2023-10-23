package com.ordermatching.config;

import com.corundumstudio.socketio.Configuration;
import com.corundumstudio.socketio.SocketIOServer;
import org.springframework.context.annotation.Bean;


@org.springframework.context.annotation.Configuration
public class WebSocketConfig {
    @Bean
    public SocketIOServer socketIOServer(){
        Configuration config = new Configuration();
        config.setHostname("localhost");
        config.setPort(9999);

        return new SocketIOServer(config);
    }
}
