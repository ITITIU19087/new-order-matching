package com.ordermatching.service.hazelcast.jet;

import com.hazelcast.internal.util.phonehome.JetInfoCollector;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;
import com.ordermatching.entity.Order;
import com.ordermatching.entity.Trade;
import com.ordermatching.entity.TradePrice;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class JetTradeService {
    @Autowired
    private JetInstance jetInstance;

    public void createTrade(Order buyOrder, Order sellOrder, double matchedQuantity){
        IMap<String, Trade> tradeMap = jetInstance.getMap("trade1");
        Trade trade = new Trade();

        trade.setUUID(UUID.randomUUID().toString());
        trade.setBuyOrderUUID(buyOrder.getUUID());
        trade.setSellOrderUUID(sellOrder.getUUID());
        trade.setQuantity(matchedQuantity);
        trade.setPrice(buyOrder.getPrice());

        trade.setTradeTime(LocalDateTime.now());

        tradeMap.put(trade.getUUID(), trade);
    }

    public List<Trade> getCandlePrice(LocalDateTime time){

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(Sources.<String, Trade>map("trades"))
                .filter(entry -> entry.getValue().getTradeTime().isAfter(time))
                .map(Map.Entry::getValue)
                .writeTo(Sinks.list("candle-trade"));

        try {
            jetInstance.newJob(pipeline).join();
            return new ArrayList<>(jetInstance.getList("candle-trade"));
        }
        finally {
            jetInstance.getList("candle-trade").destroy();
        }

    }

    public TradePrice getCandleStickPrice(){
        LocalDateTime time = LocalDateTime.now().minusMinutes(50);
        List<Trade> tradeList = getCandlePrice(time);
        Double maxPrice = Collections.max(tradeList, Comparator.comparing(Trade::getPrice)).getPrice();
        Double minPrice = Collections.min(tradeList, Comparator.comparing(Trade::getPrice)).getPrice();
        Double openPrice = Collections.min(tradeList, Comparator.comparing(Trade::getTradeTime)).getPrice();
        Double closePrice = Collections.max(tradeList, Comparator.comparing(Trade::getTradeTime)).getPrice();

        return new TradePrice(String.valueOf(time), openPrice, closePrice, maxPrice, minPrice);

    }
}
