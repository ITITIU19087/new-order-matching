package com.ordermatching.service.hazelcast;

import com.hazelcast.map.IMap;
import com.ordermatching.config.HazelcastConfig;
import com.ordermatching.entity.Order;
import com.ordermatching.entity.Trade;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Time;
import java.time.LocalDateTime;
import java.util.*;

@Service
public class TradeService {
    @Autowired
    private HazelcastConfig hazelcastConfig;

    public void createTrade(Order buyOrder, Order sellOrder, double matchedQuantity){
        IMap<String, Trade> tradeMap = hazelcastConfig.hazelcastInstance().getMap("trades");
        Trade trade = new Trade();
        String tradeUUID = UUID.randomUUID().toString();

        trade.setUUID(tradeUUID);
        trade.setBuyOrderUUID(buyOrder.getUUID());
        trade.setSellOrderUUID(sellOrder.getUUID());
        trade.setQuantity(matchedQuantity);
        trade.setPrice(buyOrder.getPrice());

        trade.setTradeTime(LocalDateTime.now());

        tradeMap.put(trade.getUUID(), trade);
    }

    public List<Trade> getUnUpdatedTrade(){
        IMap<String, Trade> tradeMap = hazelcastConfig.hazelcastInstance().getMap("trades");
        List<Trade> unUpdatedTrade = new ArrayList<>();
        for (Trade trade : tradeMap.values()) {
            if(!trade.isUpdated()){
                unUpdatedTrade.add(trade);
            }
        }
        unUpdatedTrade.sort(Comparator.comparing(Trade::getTradeTime));
        return unUpdatedTrade;
    }

    public Map<LocalDateTime, Double> getTradePrice(){
        List<Trade> unUpdatedTrade = getUnUpdatedTrade();
        Map<LocalDateTime, Double> tradePrice = new HashMap<>();
        for (Trade trade: unUpdatedTrade){
            tradePrice.put(trade.getTradeTime(), trade.getPrice());
        }
        return tradePrice;
    }
}
