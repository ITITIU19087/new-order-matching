package com.ordermatching.service.hazelcast.jet;

import com.hazelcast.internal.util.phonehome.JetInfoCollector;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.map.IMap;
import com.ordermatching.entity.Order;
import com.ordermatching.entity.Trade;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class JetTradeService {
    @Autowired
    private JetInstance jetInstance;

    public void createTrade(Order buyOrder, Order sellOrder, double matchedQuantity){
        IMap<String, Trade> tradeMap = jetInstance.getMap("trades");
        Trade trade = new Trade();

        trade.setUUID(UUID.randomUUID().toString());
        trade.setBuyOrderUUID(buyOrder.getUUID());
        trade.setSellOrderUUID(sellOrder.getUUID());
        trade.setQuantity(matchedQuantity);
        trade.setPrice(buyOrder.getPrice());

        trade.setTradeTime(LocalDateTime.now());

        tradeMap.put(trade.getUUID(), trade);
    }
}
