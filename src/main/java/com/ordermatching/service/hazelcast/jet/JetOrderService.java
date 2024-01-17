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

    private Map<String, Order> buyBatchMap;
    private Map<String, Order> sellBatchMap;

    public JetOrderService() {
        this.sellTree = new TreeSet<>();
        this.buyTree = new TreeSet<>();
        this.sellBatchMap = new HashMap<>();
        this.buyBatchMap = new HashMap<>();
    }

    public void syncPriceList(){
        IList<Double> sellPriceList = jetInstance.getList("sell-price-list");
        IList<Double> buyPriceList = jetInstance.getList("buy-price-list");

        if (!sellPriceList.isEmpty()){
            for (Double d: sellPriceList) {
                this.sellTree.add(d);
            }
        }
        if (!buyPriceList.isEmpty()){
            for (Double d: buyPriceList) {
                this.buyTree.add(d);
            }
        }
        jetInstance.getList("sell-price-list").destroy();
        jetInstance.getList("buy-price-list").destroy();
    }

    public void createOrders(List<OrderDto> orderList){
        IMap<String, Order> buyOrderMap = jetInstance.getMap("buy-orders");
        IMap<String, Order> sellOrderMap = jetInstance.getMap("sell-orders");
        IList<Double> sellPriceList = jetInstance.getList("sell-price-list");
        IList<Double> buyPriceList = jetInstance.getList("buy-price-list");
        Map<String, Order> batchMap = new HashMap<>();

        for (OrderDto orderDto : orderList) {
            Order order = convertOrder(orderDto);
            batchMap.put(order.getUUID(), order);
        }
        syncPriceList();
        for(Double d: this.buyTree){
            buyPriceList.add(d);
        }

        for(Double d : this.sellTree){
            sellPriceList.add(d);
        }

        buyOrderMap.putAll(this.buyBatchMap);
        this.buyBatchMap.clear();
        sellOrderMap.putAll(this.sellBatchMap);
        this.sellBatchMap.clear();

        matchService.initialCheck();
        matchService.proRataSell();
        matchService.proRataBuy();
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
            this.buyBatchMap.put(order.getUUID(), order);
        }
        else{
            if(orderDto.getQuantity() > 150){
                orderSellMap.put(order.getUUID(), order);
            }
            this.sellTree.add(orderDto.getPrice());
            this.sellBatchMap.put(order.getUUID(), order);
        }
        return order;
    }
}
