package com.ordermatching.service.hazelcast.jet;

import com.hazelcast.collection.IList;
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.function.ToDoubleFunctionEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.*;
import com.hazelcast.map.IMap;
import com.ordermatching.config.HazelcastConfig;
import com.ordermatching.entity.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@Slf4j
public class JetMatchService {

    @Autowired
    private JetInstance jetInstance;

    public List<Order> getAllOrders(){
        Pipeline pipeline = Pipeline.create();
        pipeline
                .readFrom(Sources.<String, Order>map("orders"))
                .filter(entry -> !entry.getValue().isMatched())
                .map(Map.Entry::getValue)
                .writeTo(Sinks.list("list"));
        jetInstance.newJob(pipeline).join();
        return new ArrayList<>(jetInstance.getList("list"));
    }

    public List<Order> getAllOrderBySide(String side){
        Pipeline pipeline = Pipeline.create();
        pipeline
                .readFrom(Sources.<String, Order>map("orders"))
                .filter(entry -> entry.getValue().getSide().equals(side))
                .map(Map.Entry::getValue)
                .sort(ComparatorEx.comparing(Order::getOrderTime))
                .writeTo(Sinks.list("list"));
        jetInstance.newJob(pipeline).join();
        return new ArrayList<>(jetInstance.getList("list"));
    }

    public Map<Double, List<Order>> groupOrderByPrice(String side){
        Pipeline pipeline = Pipeline.create();
        pipeline
                .readFrom(Sources.<String, Order>map("orders"))
                .filter(entry -> entry.getValue().getSide().equals(side))
                .groupingKey(entry -> entry.getValue().getPrice())
                .aggregate(AggregateOperations.toList())
                .writeTo(Sinks.map("groupedOrdersByPrice"));

        try{
            jetInstance.newJob(pipeline).join();
            return new HashMap<>(jetInstance.getMap("groupedOrdersByPrice"));
        }
        finally {
            jetInstance.getMap("groupedOrdersByPrice").destroy();
        }
    }

    public Double getBestPriceOfSide(String side){
        Map<Double, List<Order>> priceMap = groupOrderByPrice(side);
        if (priceMap.isEmpty()){
            return 0.0;
        }
        if (side.equals("BUY")){
            return Collections.max(priceMap.keySet());
        }
        else{
            return Collections.min(priceMap.keySet());
        }
    }
}
