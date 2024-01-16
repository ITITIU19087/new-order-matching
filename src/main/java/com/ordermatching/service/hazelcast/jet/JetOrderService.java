package com.ordermatching.service.hazelcast.jet;

import com.hazelcast.collection.IList;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.map.IMap;
import com.ordermatching.dto.OrderDto;
import com.ordermatching.entity.Order;
import com.ordermatching.service.socketio.SocketIOService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.yaml.snakeyaml.DumperOptions;

import java.time.LocalDateTime;
import java.util.*;

@Service
@CrossOrigin(origins = "http://localhost:3000")
public class JetOrderService {
    @Autowired
    private JetInstance jetInstance;

    @Autowired
    private JetMatchService matchService;

    @Autowired
    private SocketIOService service;

    private TreeSet<Double> sellTree;
    private TreeSet<Double> buyTree;

    public JetOrderService() {
        this.sellTree = new TreeSet<>();
        this.buyTree = new TreeSet<>();
    }

    public void createOrders(List<OrderDto> orderList){
        IMap<String, Order> orderMap = jetInstance.getMap("orders1");
        IList<Double> sellPriceList = jetInstance.getList("sell-price-list");
        IList<Double> buyPriceList = jetInstance.getList("buy-price-list");
        Map<String, Order> batchMap = new HashMap<>();

        for (OrderDto orderDto : orderList) {
            Order order = convertOrder(orderDto);
            batchMap.put(order.getUUID(), order);
        }
        for(Double d: this.buyTree){
            buyPriceList.add(d);
        }

        for(Double d : this.sellTree){
            sellPriceList.add(d);
        }

        orderMap.putAll(batchMap);

//        matchService.initialCheck();
//        matchService.proRataSell();
//        matchService.proRataBuy();
        matchService.matchOrdersUsingFifo();

        service.notifyOrderCreation(matchService.getTotalOrderAtPrice("BUY"), matchService.getTotalOrderAtPrice("SELL"));
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
        if(orderDto.getSide().equals("BUY")){
            if(orderDto.getQuantity() > 150){
                orderMap.put(order.getUUID(), order);
            }
            this.buyTree.add(orderDto.getPrice());
        }
        else{
            if(orderDto.getQuantity() > 150){
                orderSellMap.put(order.getUUID(), order);
            }
            this.sellTree.add(orderDto.getPrice());
        }
        return order;
    }
}
