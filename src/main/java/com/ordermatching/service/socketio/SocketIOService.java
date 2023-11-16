package com.ordermatching.service.socketio;

import com.corundumstudio.socketio.SocketIOServer;
import com.ordermatching.entity.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class SocketIOService {
    private SocketIOServer server;


    @Autowired
    public SocketIOService(SocketIOServer server) {
        this.server = server;
    }

    public void notifyOrderCreation(String message) {
        server.getBroadcastOperations().sendEvent("orderCreated", message);
    }
}
