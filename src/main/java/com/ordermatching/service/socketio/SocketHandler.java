package com.ordermatching.service.socketio;

import com.corundumstudio.socketio.SocketIOServer;
import com.ordermatching.config.WebSocketConfig;
import com.ordermatching.service.hazelcast.MatchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Map;

@Component
public class SocketHandler {

    @Autowired
    private SocketIOServer socketIOServer;

    @Autowired
    private MatchService matchService;

    @PostConstruct
    public void start() {
        socketIOServer.addConnectListener(client -> {
            Map<Double, Integer> map = matchService.getTotalOrderAtPrice("BUY");
            client.sendEvent(map.keySet().toString(), map.values());
        });

        socketIOServer.start();
    }

    @PreDestroy
    public void stop() {
        socketIOServer.stop();
    }
}
