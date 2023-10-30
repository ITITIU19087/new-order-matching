package com.ordermatching.service.hazelcast;

import com.hazelcast.map.IMap;
import com.ordermatching.config.HazelcastConfig;
import com.ordermatching.dto.OrderDto;
import com.ordermatching.entity.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
public class OrderService {
    @Autowired
    private HazelcastConfig hazelcastConfig;

    @Autowired
    private MatchService matchService;

    public void createOrder(List<OrderDto> orderList){
        IMap<String, Order> orderMap = hazelcastConfig.hazelcastInstance().getMap("orders");
        for(OrderDto orderDto: orderList){
            Order order = new Order();
            String orderId = UUID.randomUUID().toString();
            order.setUUID(orderId);
            order.setPrice(orderDto.getPrice());
            order.setQuantity(orderDto.getQuantity());
            order.setSide(orderDto.getSide());
            order.setOrderTime(LocalDateTime.now());
            order.setStatus("Success");
            orderMap.put(order.getUUID(), order);
        }
        matchService.proRataSell();
        matchService.proRataBuy();
        matchService.matchOrdersUsingFifo();
    }
}
