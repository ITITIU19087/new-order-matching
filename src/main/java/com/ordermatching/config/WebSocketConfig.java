package com.ordermatching.config;

import com.corundumstudio.socketio.Configuration;
import com.corundumstudio.socketio.SocketIOServer;
import org.springframework.context.annotation.Bean;


@org.springframework.context.annotation.Configuration
public class WebSocketConfig {
    @Bean(destroyMethod = "stop")
    public SocketIOServer socketIOServer(){
        Configuration config = new Configuration();
        config.setHostname("localhost");
        config.setPort(9999);

        SocketIOServer server =  new SocketIOServer(config);

        server.start();

        return server;
    }
}
