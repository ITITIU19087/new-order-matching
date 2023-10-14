package com.ordermatching.service.hazelcast;

import com.hazelcast.map.IMap;
import com.ordermatching.config.HazelcastConfig;
import com.ordermatching.entity.Order;
import com.ordermatching.entity.Trade;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class TradeService {
    @Autowired
    private HazelcastConfig hazelcastConfig;

    public void createTrade(Order buyOrder, Order sellOrder, double matchedQuantity){
        IMap<String, Trade> orderMap = hazelcastConfig.hazelcastInstance().getMap("trades");
        Trade trade = new Trade();
        String tradeUUID = UUID.randomUUID().toString();

        trade.setUUID(tradeUUID);
        trade.setBuyOrderUUID(buyOrder.getUUID());
        trade.setSellOrderUUID(sellOrder.getUUID());
        trade.setQuantity(matchedQuantity);
        trade.setPrice(buyOrder.getPrice());

        orderMap.put(trade.getUUID(), trade);
    }
}
