package com.ordermatching.config;

import com.corundumstudio.socketio.Configuration;
import com.corundumstudio.socketio.SocketIOClient;
import com.corundumstudio.socketio.SocketIOServer;
import com.corundumstudio.socketio.listener.ConnectListener;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;


@org.springframework.context.annotation.Configuration
public class WebSocketConfig{
    @Bean
    public SocketIOServer socketIOServer(){
        Configuration config = new Configuration();
        config.setHostname("localhost");
        config.setPort(9999);
        config.setOrigin("http://localhost:3000"); // Allows connections from any origin

        final SocketIOServer server = new SocketIOServer(config);

        server.addConnectListener(new ConnectListener() {
            @Override
            public void onConnect(SocketIOClient client) {

            }
        });

        server.start();
        return server;
    }
}
