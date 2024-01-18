package com.ordermatching.service.hazelcast.jet;

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
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
    private HazelcastInstance hazelcastInstance;

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

    public void syncPriceList() {
        IList<Double> sellPriceList = hazelcastInstance.getList("sell-price-list");
        IList<Double> buyPriceList = hazelcastInstance.getList("buy-price-list");

        IList<Double> curSellPriceList = hazelcastInstance.getList("curr-sell-price");
        IList<Double> curBuyPriceList = hazelcastInstance.getList("curr-buy-price");

        if (!hazelcastInstance.getMap("buyMap").isEmpty()) {
            for (Double d : this.buyTree) {
                curBuyPriceList.add(d);
            }
        }

        if (!hazelcastInstance.getMap("sellMap").isEmpty()) {
            for (Double d : this.sellTree) {
                curSellPriceList.add(d);
            }
        }

        if (!sellPriceList.isEmpty()) {
            for (Double d : sellPriceList) {
                this.sellTree.add(d);
            }
        }
        if (!buyPriceList.isEmpty()) {
            for (Double d : buyPriceList) {
                this.buyTree.add(d);
            }
        }
        hazelcastInstance.getList("sell-price-list").destroy();
        hazelcastInstance.getList("buy-price-list").destroy();
    }

    public void createOrders(List<OrderDto> orderList) {
        IMap<String, Order> buyOrderMap = hazelcastInstance.getMap("buy-orders");
        IMap<String, Order> sellOrderMap = hazelcastInstance.getMap("sell-orders");
        IList<Double> sellPriceList = hazelcastInstance.getList("sell-price-list");
        IList<Double> buyPriceList = hazelcastInstance.getList("buy-price-list");


        Double bestBuyPrice = matchService.getBestBuyPrice();
        Double bestSellPrice = matchService.getBestSellPrice();

        for (OrderDto orderDto : orderList) {
            convertOrder(orderDto, bestBuyPrice, bestSellPrice);
        }
        syncPriceList();
        for (Double d : this.buyTree) {
            buyPriceList.add(d);
        }

        for (Double d : this.sellTree) {
            sellPriceList.add(d);
        }

        buyOrderMap.putAll(this.buyBatchMap);
        this.buyBatchMap.clear();
        sellOrderMap.putAll(this.sellBatchMap);
        this.sellBatchMap.clear();

        boolean isBuyEmpty = hazelcastInstance.getMap("buyMap").isEmpty();
        boolean isSellEmpty = hazelcastInstance.getMap("sellMap").isEmpty();

        processRequest(isBuyEmpty, isSellEmpty);
//        service.notifyOrderCreation(matchService.getTotalOrderAtPrice("BUY"), matchService.getTotalOrderAtPrice("SELL"));
    }
    public void processRequest ( boolean isBuyEmpty, boolean isSellEmpty){
        if (!isBuyEmpty && !isSellEmpty) {
            matchService.initialCheck(false);
            matchService.proRataSell(false);
            matchService.proRataBuy(false);
            matchService.matchOrdersUsingFifo(false);
            matchService.syncOrderMap();
        }
        if (!isBuyEmpty && isSellEmpty) {
            Double bestBuyPrice = matchService.getBestBuyPrice();
            Double bestSellPrice = matchService.getBestSellPrice();
             if (bestSellPrice > bestBuyPrice) {
                 matchService.syncOrderMap();
             } else if (bestBuyPrice.equals(bestSellPrice)) {
                 matchService.proRataSell(true);
                 matchService.matchOrdersUsingFifoVer2(1);
                 matchService.syncOrderMap();
             } else {
                 matchService.initCheckVer2(1);
                 matchService.proRataSell(true);
                 matchService.matchOrdersUsingFifoVer2(1);
                 matchService.syncOrderMap();
             }

        } else if (isBuyEmpty && !isSellEmpty) {
            Double bestBuyPrice = matchService.getBestBuyPrice();
            Double bestSellPrice = matchService.getBestSellPrice();
            if (bestSellPrice > bestBuyPrice) {
                matchService.syncOrderMap();
            } else if (bestBuyPrice.equals(bestSellPrice)) {
                matchService.proRataBuy(true);
                matchService.matchOrdersUsingFifoVer2(2);
                matchService.syncOrderMap();
            } else {
                matchService.initCheckVer2(2);
                matchService.proRataBuy(true);
                matchService.matchOrdersUsingFifoVer2(2);
                matchService.syncOrderMap();
            }
        }
    }

    public Order convertOrder (OrderDto orderDto, Double bestBuyPrice, Double bestSellPrice){
        IMap<String, Order> orderMap = hazelcastInstance.getMap("orders_prorata_buy");
        IMap<String, Order> orderSellMap = hazelcastInstance.getMap("orders_prorata_sell");
        IMap<String, Order> buyMap = hazelcastInstance.getMap("buyMap");
        IMap<String, Order> sellMap = hazelcastInstance.getMap("sellMap");

        Order order = new Order();
        String orderId = UUID.randomUUID().toString();
        order.setUUID(orderId);
        order.setPrice(orderDto.getPrice());
        order.setQuantity(orderDto.getQuantity());
        order.setSide(orderDto.getSide());
        order.setOrderTime(LocalDateTime.now());
        order.setStatus("Success");
        if (orderDto.getSide().equals("BUY")) {
            if (orderDto.getQuantity() > 150) {
                orderMap.put(order.getUUID(), order);
            }
            if (orderDto.getPrice() > bestBuyPrice && bestBuyPrice != 0) {
                buyMap.put(order.getUUID(), order);
            } else {
                this.buyBatchMap.put(order.getUUID(), order);
            }
            this.buyTree.add(orderDto.getPrice());
        } else {
            if (orderDto.getQuantity() > 150) {
                orderSellMap.put(order.getUUID(), order);
            }
            if (orderDto.getPrice() < bestSellPrice) {
                sellMap.put(order.getUUID(), order);
            } else {
                this.sellBatchMap.put(order.getUUID(), order);
            }
            this.sellTree.add(orderDto.getPrice());
        }
        return order;
    }
}