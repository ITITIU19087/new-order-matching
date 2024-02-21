package com.ordermatching.service.hazelcast.newjet;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.ordermatching.entity.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class JetAggregateDataService {
    @Autowired
    private HazelcastInstance hazelcastInstance;

    @Autowired
    private JetService jetService;

    public Map<Double, Long> aggregateData(String side){
        Pipeline pipeline = Pipeline.create();
        if(side.equals("BUY")){
            pipeline
                    .readFrom(Sources.<String, Order>map("new_coming_buy_orders"))
                    .groupingKey(entry -> entry.getValue().getPrice())
                    .aggregate(AggregateOperations.counting())
                    .writeTo(Sinks.map("total-order"));
        }
        else{
            pipeline
                    .readFrom(Sources.<String, Order>map("new_coming_sell_orders"))
                    .groupingKey(entry -> entry.getValue().getPrice())
                    .aggregate(AggregateOperations.counting())
                    .writeTo(Sinks.map("total-order-at-price"));
        }
        try{
            jetService.newJob(pipeline).join();
            return new HashMap<>(hazelcastInstance.getMap("total-order-at-price"));
        }
        finally {
            hazelcastInstance.getMap("total-order-at-price").destroy();
        }
    }
}
