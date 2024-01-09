package com.ordermatching.service.hazelcast.jet;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.map.IMap;
import com.ordermatching.dto.OrderDto;
import com.ordermatching.entity.Order;
import com.ordermatching.service.socketio.SocketIOService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
public class JetOrderService {
    @Autowired
    private JetInstance jetInstance;

    @Autowired
    private JetMatchService matchService;

    @Autowired
    private SocketIOService service;

    public void createOrders(List<OrderDto> orderList){
        IMap<String, Order> orderMap = jetInstance.getMap("orders1");
        for(OrderDto orderDto: orderList){
            Order order = convertOrder(orderDto);
            orderMap.put(order.getUUID(), order);
        }
        matchService.initialCheck();
        matchService.proRataSell();
        matchService.proRataBuy();
        matchService.matchOrdersUsingFifo();

        service.notifyOrderCreation("orderCreated");
    }
    public Order convertOrder(OrderDto orderDto){
        IMap<String, Order> orderMap = jetInstance.getMap("orders_prorata_buy");
        IMap<String, Order> orderSellMap = jetInstance.getMap("orders_prorata_sell");
        Order order = new Order();
        String orderId = UUID.randomUUID().toString();
        order.setUUID(orderId);
        order.setPrice(orderDto.getPrice());
        order.setQuantity(orderDto.getQuantity());
        order.setSide(orderDto.getSide());
        order.setOrderTime(LocalDateTime.now());
        order.setStatus("Success");
        if(orderDto.getQuantity() >= 150){
            if(orderDto.getSide().equals("BUY")){
                orderMap.put(order.getUUID(), order);
            }
            else {
                orderSellMap.put(order.getUUID(), order);
            }
        }
        return order;
    }
}
