package com.ordermatching.service.socketio;

import com.corundumstudio.socketio.SocketIOServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class SocketIOService {
    private SocketIOServer server;

    @Autowired
    public SocketIOService(SocketIOServer server) {
        this.server = server;
    }

    public void notifyOrderCreation(Map<Double, Long> msg1, Map<Double, Long> msg2) {
        server.getBroadcastOperations().sendEvent("buyEvent", msg1);
        server.getBroadcastOperations().sendEvent("sellEvent",msg2);
    }

    public void dumps(String msg) {
        server.getBroadcastOperations().sendEvent("buyEvent", msg);
    }
}
