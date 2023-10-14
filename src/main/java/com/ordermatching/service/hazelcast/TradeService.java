package com.ordermatching.service.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.map.IMap;
import com.ordermatching.config.HazelcastConfig;
import com.ordermatching.entity.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class TradeService {
    @Autowired
    private HazelcastConfig hazelcastConfig;

    public List<Order> getAllOrders(){
        IMap<String, Order> orderMap = hazelcastConfig.hazelcastInstance().getMap("orders");
        return new ArrayList<>(orderMap.values());
    }

    public List<Order> getAllOrdersBySide(String side){
        IMap<String, Order> orderMap = hazelcastConfig.hazelcastInstance().getMap("orders");
        List<Order> orderListBySide = new ArrayList<>();
        for (Order order: orderMap.values()){
            if(order.getSide().equals(side)){
                orderListBySide.add(order);
            }
        }
        return orderListBySide;
    }

    public Map<Double, List<Order>> groupOrderByPrice(String side){
        IMap<String, Order> orderMap = hazelcastConfig.hazelcastInstance().getMap("orders");
        Map<Double, List<Order>> ordersGroupedByPrice = new HashMap<>();

        for (Order order : orderMap.values()) {
            if (order.getSide().equals(side)) {
                double price = order.getPrice();
                List<Order> ordersWithSamePrice = ordersGroupedByPrice.getOrDefault(price, new ArrayList<>());
                ordersWithSamePrice.add(order);
                ordersGroupedByPrice.put(price, ordersWithSamePrice);
            }
        }
        return ordersGroupedByPrice;
    }
}
