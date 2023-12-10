package com.ordermatching.service.hazelcast.jet;

import com.hazelcast.collection.IList;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.*;
import com.hazelcast.map.IMap;
import com.ordermatching.config.HazelcastConfig;
import com.ordermatching.entity.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class JetMatchService {

    @Autowired
    private HazelcastConfig hazelcastConfig;

    public List<Object> getAllOrders(){
        JetInstance jetInstance = hazelcastConfig.jetInstance();
        Pipeline pipeline = Pipeline.create();
        pipeline
                .readFrom(Sources.<String, Order>map("orders"))
                .filter(entry -> !entry.getValue().isMatched())
                .map(Map.Entry::getValue)
                .writeTo(Sinks.list("list"));

        jetInstance.newJob(pipeline).join();
        return new ArrayList<>(jetInstance.getList("list"));
    }


}
