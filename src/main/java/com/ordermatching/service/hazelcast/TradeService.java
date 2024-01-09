package com.ordermatching.service.hazelcast;

import com.hazelcast.map.IMap;
import com.ordermatching.config.HazelcastConfig;
import com.ordermatching.entity.Order;
import com.ordermatching.entity.Trade;
import com.ordermatching.entity.TradePrice;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

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

    public List<Trade> getCandlePrice(LocalDateTime time){
        IMap<String, Trade> tradeMap = hazelcastConfig.hazelcastInstance().getMap("trades");
        List<Trade> tradeList = new ArrayList<>(tradeMap.values())
                .stream()
                .filter(trade -> trade.getTradeTime().isAfter(time))
                .collect(Collectors.toList());
        return tradeList;

    }

    public TradePrice getCandleStickPrice(){
        LocalDateTime time = LocalDateTime.now().minusMinutes(2);
        List<Trade> tradeList = getCandlePrice(time);
        Double    maxPrice = Collections.max(tradeList, Comparator.comparing(Trade::getPrice)).getPrice();
        Double    minPrice = Collections.min(tradeList, Comparator.comparing(Trade::getPrice)).getPrice();
        Double    openPrice = Collections.min(tradeList, Comparator.comparing(Trade::getTradeTime)).getPrice();
        Double    closePrice = Collections.max(tradeList, Comparator.comparing(Trade::getTradeTime)).getPrice();

        return new TradePrice(String.valueOf(time), openPrice, closePrice, maxPrice, minPrice);

    }

}
