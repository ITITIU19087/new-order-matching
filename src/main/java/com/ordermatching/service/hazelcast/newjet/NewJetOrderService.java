package com.ordermatching.service.hazelcast.newjet;

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.ordermatching.dto.OrderDto;
import com.ordermatching.entity.Order;
import com.ordermatching.service.socketio.SocketIOService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;

@Service
public class NewJetOrderService {

    @Autowired
    private NewJetMatchService matchService;

    @Autowired
    private HazelcastInstance hazelcastInstance;

    @Autowired
    private SocketIOService service;

    private TreeSet<Double> sellTree;
    private TreeSet<Double> buyTree;

    private Map<String, Order> buyBatchMap;
    private Map<String, Order> sellBatchMap;

    public NewJetOrderService() {
        this.sellTree = new TreeSet<>();
        this.buyTree = new TreeSet<>();
        this.sellBatchMap = new HashMap<>();
        this.buyBatchMap = new HashMap<>();
    }

    public void syncPriceList(){
        IList<Double> buyPrice = hazelcastInstance.getList("buy_price_list");
        IList<Double> sellPrice = hazelcastInstance.getList("sell_price_list");

        if(!buyPrice.isEmpty()){
            for(Double price : buyPrice){
                this.buyTree.add(price);
            }
        }
        if(!sellPrice.isEmpty()){
            for(Double price: sellPrice){
                this.sellTree.add(price);
            }
        }
        buyPrice.clear();
        sellPrice.clear();

        for(Double price : this.buyTree){
            buyPrice.add(price);
        }
        for (Double price : this.sellTree){
            sellPrice.add(price);
        }

    }

    public void createOrders(List<OrderDto> orderList){
        IMap<String, Order> buyMap = hazelcastInstance.getMap("waiting_buy_orders");
        IMap<String, Order> sellMap = hazelcastInstance.getMap("waiting_sell_orders");

        IMap<String, Order> newBuyMap = hazelcastInstance.getMap("new_coming_buy_orders");
        IMap<String, Order> newSellMap = hazelcastInstance.getMap("new_coming_sell_orders");

        Double bestBuyPrice = matchService.getBestPrice("BUY");
        Double bestSellPrice = matchService.getBestPrice("SELL");

        this.buyTree.clear();
        this.sellTree.clear();

        for (OrderDto orderDto : orderList) {
            convertOrder(orderDto, bestBuyPrice, bestSellPrice);
        }

        syncPriceList();

        buyMap.putAll(this.buyBatchMap);
        this.buyBatchMap.clear();
        sellMap.putAll(this.sellBatchMap);
        this.sellBatchMap.clear();


        boolean isBuyEmpty = hazelcastInstance.getMap("eval_buy_orders").isEmpty();
        boolean isSellEmpty = hazelcastInstance.getMap("eval_sell_orders").isEmpty();


        processOrderRequest(isBuyEmpty, isSellEmpty);

        service.notifyOrderCreation(matchService.sumOfOrdersAtPrice("BUY"), matchService.sumOfOrdersAtPrice("SELL"));

    }

    public Order convertOrder (OrderDto orderDto, Double bestBuyPrice, Double bestSellPrice){

        IMap<String, Order> buyMap = hazelcastInstance.getMap("large_buy_orders");
        IMap<String, Order> sellMap = hazelcastInstance.getMap("large_sell_orders");

        IMap<String, Order> evalBuyMap = hazelcastInstance.getMap("eval_buy_orders");
        IMap<String, Order> evalSellMap = hazelcastInstance.getMap("eval_sell_orders");

        Order order = new Order();
        String orderId = UUID.randomUUID().toString();
        order.setUUID(orderId);
        order.setPrice(orderDto.getPrice());
        order.setQuantity(orderDto.getQuantity());
        order.setSide(orderDto.getSide());
        order.setOrderTime(LocalDateTime.now());


        if (order.getSide().equals("BUY")){
            if(order.getQuantity() > 150){
                buyMap.put(order.getUUID(), order);
            }
            if (order.getPrice() > bestBuyPrice && bestBuyPrice != 0){
                evalBuyMap.put(order.getUUID(), order);
            }
            else{
                this.buyBatchMap.put(order.getUUID(), order);
            }
            this.buyTree.add(order.getPrice());
        }
        else{
            if(order.getQuantity() > 150){
                sellMap.put(order.getUUID(), order);
            }
            if (order.getPrice() < bestSellPrice){
                evalSellMap.put(order.getUUID(), order);
            }
            else{
                this.sellBatchMap.put(order.getUUID(), order);
            }
            this.sellTree.add(order.getPrice());
        }

        return order;
    }
    public int evaluationCode(boolean isBuyEmpty, boolean isSellEmpty){
        //Default case
        if (isBuyEmpty && isSellEmpty){
            //Case 0: Both Buy and Sell empty
            return 0;
        } else if (!isBuyEmpty && isSellEmpty) {
            //Case 1: new Buy but no Sell
            //Take Sell from WS
            return 1;
        }
        else if(isBuyEmpty && !isSellEmpty){
            //Case 2: no Buy but new Sell
            //Take Buy from WS
            return 2;
        }
        else{
            // Case 3: Both new Buy and Sell
            return 3;
        }
    }

    public void processOrderRequest(boolean isBuyEmpty, boolean isSellEmpty){
        int executionCode = evaluationCode(isBuyEmpty, isSellEmpty);


        matchService.aggressiveOrderCheck(executionCode);
        matchService.proRataBuyExe(executionCode);
        matchService.proRataSellExe(executionCode);
        matchService.fifoExe(executionCode);

        matchService.syncOrderMaps();

    }

    public List<Double> check(String side){
        List<Double> dl = new ArrayList<>();
        if (side.equals("BUY")){
            for(Double d : this.buyTree){
                dl.add(d);
            }
        }
        else if(side.equals("SELL")){
            for(Double d : this.sellTree){
                dl.add(d);
            }
        }
        return dl;
    }
}
