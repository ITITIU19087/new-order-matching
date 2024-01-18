package com.ordermatching.service.hazelcast.jet;

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.*;
import com.hazelcast.map.IMap;
import com.ordermatching.entity.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@Slf4j
public class JetMatchService {
    private static final Double PRO_RATA_MIN_ALLOCATION = 1.0;
    @Autowired
    private HazelcastInstance hazelcastInstance;

    @Autowired
    private JetService jetService;

    @Autowired
    private JetTradeService tradeService;


    public List<Order> getOrdersBySide(String side) {
        Pipeline pipeline = Pipeline.create();
        if(side.equals("BUY")){
            pipeline
                    .readFrom(Sources.<String, Order>map("buy-orders"))
                    .filter(entry -> !entry.getValue().isMatched())
                    .filter(entry -> entry.getValue().getSide().equals(side))
                    .map(Map.Entry::getValue)
                    .writeTo(Sinks.list("list"));
        }
        else{
            pipeline
                    .readFrom(Sources.<String, Order>map("sell-orders"))
                    .filter(entry -> !entry.getValue().isMatched())
                    .filter(entry -> entry.getValue().getSide().equals(side))
                    .map(Map.Entry::getValue)
                    .writeTo(Sinks.list("list"));
        }
        try {
            jetService.newJob(pipeline).join();
            return new ArrayList<>(hazelcastInstance.getList("list"));
        } finally {
            hazelcastInstance.getList("list").destroy();
        }
    }

    public Map<Double, List<Order>> groupOrderByPrice(String side) {
        Pipeline pipeline = Pipeline.create();
        pipeline
                .readFrom(Sources.<String, Order>map("orders1"))
                .filter(entry -> entry.getValue().getSide().equals(side))
                .filter(entry -> !entry.getValue().isMatched())
                .groupingKey(entry -> entry.getValue().getPrice())
                .aggregate(AggregateOperations.toList())
                .writeTo(Sinks.map("groupedOrdersByPrice"));
        jetService.newJob(pipeline).join();
        return new HashMap<>(hazelcastInstance.getMap("groupedOrdersByPrice"));
    }

    public List<Order> getOrdersAtPrice(String side, Double price, boolean version2) {
        String buyMap;
        String sellMap;

        if(version2){
            buyMap = "buy-orders";
            sellMap = "sell-orders";
        }
        else{
            buyMap = "buyMap";
            sellMap = "sellMap";
        }

        Pipeline pipeline = Pipeline.create();
        if (side.equals("BUY")){
            pipeline
                    .readFrom(Sources.<String, Order>map(buyMap))
                    .filter(entry -> entry.getValue().getPrice().equals(price))
                    .map(Map.Entry::getValue)
                    .sort(ComparatorEx.comparing(Order::getOrderTime))
                    .writeTo(Sinks.list("orderListAtPrice"));
        }
        else{
            pipeline
                    .readFrom(Sources.<String, Order>map(sellMap))
                    .filter(entry -> entry.getValue().getPrice().equals(price))
                    .map(Map.Entry::getValue)
                    .sort(ComparatorEx.comparing(Order::getOrderTime))
                    .writeTo(Sinks.list("orderListAtPrice"));
        }
        try {
            jetService.newJob(pipeline).join();
            return new ArrayList<>(hazelcastInstance.getList("orderListAtPrice"));

        } finally {
            hazelcastInstance.getList("orderListAtPrice").destroy();
        }
    }

    public void updateOrder(Order order) {
        IMap<String, Order> buyOrderMap = hazelcastInstance.getMap("buy-orders");
        IMap<String, Order> sellOrderMap = hazelcastInstance.getMap("sell-orders");
        IMap<String, Order> matchedOrderMap = hazelcastInstance.getMap("matched-orders");
        IMap<String, Order> orderSell = hazelcastInstance.getMap("orders_prorata_sell");
        IMap<String, Order> orderBuy = hazelcastInstance.getMap("orders_prorata_buy");

        IMap<String, Order> buyMap = hazelcastInstance.getMap("buyMap");
        IMap<String, Order> sellMap = hazelcastInstance.getMap("sellMap");

        buyOrderMap.replace(order.getUUID(), order);
        sellOrderMap.replace(order.getUUID(), order);
        orderSell.replace(order.getUUID(), order);
        orderBuy.replace(order.getUUID(), order);
        buyMap.replace(order.getUUID(), order);
        sellMap.replace(order.getUUID(), order);
        if (order.isMatched()) {
            orderSell.remove(order.getUUID());
            orderBuy.remove(order.getUUID());
            buyOrderMap.remove(order.getUUID());
            sellOrderMap.remove(order.getUUID());
            buyMap.remove(order.getUUID());
            sellMap.remove(order.getUUID());
            matchedOrderMap.put(order.getUUID(), order);
        }
    }

    public void executeTrade(Order buyOrder, Order sellOrder) {
        Double buyQuantity = buyOrder.getQuantity();
        Double sellQuantity = sellOrder.getQuantity();

        if ((buyQuantity - sellQuantity) == 0) {
            buyOrder.setMatched(true);
            buyOrder.setQuantity(0.0);

            sellOrder.setMatched(true);
            sellOrder.setQuantity(0.0);

            tradeService.createTrade(buyOrder, sellOrder, buyQuantity);
            updateOrder(buyOrder);
            updateOrder(sellOrder);
        } else if ((buyQuantity - sellQuantity) > 0) {
            buyOrder.setQuantity(buyQuantity - sellQuantity);

            sellOrder.setMatched(true);
            sellOrder.setQuantity(0.0);

            tradeService.createTrade(buyOrder, sellOrder, sellQuantity);
            updateOrder(buyOrder);
            updateOrder(sellOrder);
        } else {
            buyOrder.setMatched(true);
            buyOrder.setQuantity(0.0);

            sellOrder.setQuantity(sellQuantity - buyQuantity);

            tradeService.createTrade(buyOrder, sellOrder, buyQuantity);
            updateOrder(buyOrder);
            updateOrder(sellOrder);
        }
    }

    public void matchOrders(List<Order> buyOrders, List<Order> sellOrders) {
        while (buyOrders.iterator().hasNext() && sellOrders.iterator().hasNext()) {
            Order buyOrder = buyOrders.iterator().next();
            Order sellOrder = sellOrders.iterator().next();

            executeTrade(buyOrder, sellOrder);

            if (buyOrder.isMatched()) {
                buyOrders.remove(buyOrder);
            }
            if (sellOrder.isMatched()) {
                sellOrders.remove(sellOrder);
            }
        }
    }

    public void matchOrdersUsingFifo(boolean version) {
        double bestBuyPrice = getBestBuyPrice();
        double bestSellPrice = getBestSellPrice();

        while (bestSellPrice == bestBuyPrice) {
            List<Order> buyOrders = getOrdersAtPrice("BUY", bestBuyPrice, version);
            List<Order> sellOrders = getOrdersAtPrice("SELL", bestSellPrice, version);

            matchOrders(buyOrders, sellOrders);

            updatePriceList("BUY", bestBuyPrice, version);
            updatePriceList("SELL", bestSellPrice, version);

            bestBuyPrice = getBestBuyPrice();
            bestSellPrice = getBestSellPrice();
        }
    }

    //version2 param: if true, get data from stored Map, else get data from pre stored Map
    public void matchOrdersUsingFifoVer2(int executeCase) {
        double bestBuyPrice = getBestBuyPrice();
        double bestSellPrice = getBestSellPrice();

        if (executeCase == 1) {
            while (bestSellPrice == bestBuyPrice) {
                List<Order> buyOrders = getOrdersAtPrice("BUY", bestBuyPrice, false);
                List<Order> sellOrders = getOrdersAtPrice("SELL", bestSellPrice, true);

                matchOrders(buyOrders, sellOrders);

                updatePriceList("BUY", bestBuyPrice, false);
                updatePriceList("SELL", bestSellPrice, true);

                bestBuyPrice = getBestBuyPrice();
                bestSellPrice = getBestSellPrice();
            }
        } else if (executeCase == 2) {
            while (bestSellPrice == bestBuyPrice) {
                List<Order> buyOrders = getOrdersAtPrice("BUY", bestBuyPrice, true);
                List<Order> sellOrders = getOrdersAtPrice("SELL", bestSellPrice, false);

                matchOrders(buyOrders, sellOrders);

                updatePriceList("BUY", bestBuyPrice, true);
                updatePriceList("SELL", bestSellPrice, false);

                bestBuyPrice = getBestBuyPrice();
                bestSellPrice = getBestSellPrice();
            }
        }
    }

    public void initCheckVer2(int executeCase){
        double bestBuyPrice = getBestBuyPrice();
        double bestSellPrice = getBestSellPrice();

        if (executeCase == 1) {
            while (bestSellPrice < bestBuyPrice) {
                List<Order> buyOrders = getOrdersAtPrice("BUY", bestBuyPrice, false);
                List<Order> sellOrders = getOrdersAtPrice("SELL", bestSellPrice, true);

                matchOrders(buyOrders, sellOrders);

                updatePriceList("BUY", bestBuyPrice, false);
                updatePriceList("SELL", bestSellPrice, true);

                bestBuyPrice = getBestBuyPrice();
                bestSellPrice = getBestSellPrice();
            }
        } else if (executeCase == 2) {
            while (bestSellPrice < bestBuyPrice) {
                List<Order> buyOrders = getOrdersAtPrice("BUY", bestBuyPrice, true);
                List<Order> sellOrders = getOrdersAtPrice("SELL", bestSellPrice, false);

                matchOrders(buyOrders, sellOrders);

                updatePriceList("BUY", bestBuyPrice, true);
                updatePriceList("SELL", bestSellPrice, false);

                bestBuyPrice = getBestBuyPrice();
                bestSellPrice = getBestSellPrice();
            }
        }
    }

    public void initialCheck(boolean version) {
        double bestBuyPrice = getBestBuyPrice();
        double bestSellPrice = getBestSellPrice();

        while (bestSellPrice < bestBuyPrice) {
            List<Order> buyOrders = getOrdersAtPrice("BUY", bestBuyPrice, version);
            List<Order> sellOrders = getOrdersAtPrice("SELL", bestSellPrice, version);

            matchOrders(buyOrders, sellOrders);

            updatePriceList("BUY", bestBuyPrice, version);
            updatePriceList("SELL", bestSellPrice, version);

            bestBuyPrice = getBestBuyPrice();
            bestSellPrice = getBestSellPrice();
        }
    }
    public List<Order> getProRataOrder(String side) {
        Pipeline pipeline = Pipeline.create();
        try{
            if (side.equals("BUY")) {
                pipeline
                        .readFrom(Sources.<String, Order>map("orders_prorata_buy"))
                        .filter(entry -> !entry.getValue().isMatched())
                        .map(Map.Entry::getValue)
                        .writeTo(Sinks.list("prorata_buy_list"));
                jetService.newJob(pipeline).join();
                return new ArrayList<>(hazelcastInstance.getList("prorata_buy_list"));
            } else {
                pipeline
                        .readFrom(Sources.<String, Order>map("orders_prorata_sell"))
                        .filter(entry -> !entry.getValue().isMatched())
                        .map(Map.Entry::getValue)
                        .writeTo(Sinks.list("prorata_sell_list"));
                jetService.newJob(pipeline).join();
                return new ArrayList<>(hazelcastInstance.getList("prorata_sell_list"));
            }
        }
         finally {
            hazelcastInstance.getList("prorata_sell_list").destroy();
            hazelcastInstance.getList("prorata_buy_list").destroy();
        }
    }

    public void proRataSell(boolean version) {
        List<Order> orderProRataList = getProRataOrder("SELL");

        if (!orderProRataList.isEmpty()) {
            Double bestBuyPrice = getBestBuyPrice();
            Double bestSellPrice = getBestSellPrice();
            if (bestBuyPrice.equals(bestSellPrice)) {
                List<Order> buyOrders = getOrdersAtPrice("BUY", bestBuyPrice, version);
                Order sellOrder = orderProRataList.get(0);
                while (sellOrder.getQuantity() > 0) {
                    buyOrders.sort(Comparator.comparing(Order::getQuantity).reversed());
                    double totalBuyQuantity = buyOrders.stream().mapToDouble(Order::getQuantity).sum();
                    double totalSellQuantity = sellOrder.getQuantity();
                    for (Order order : buyOrders) {
                        double ratio = order.getQuantity() / totalBuyQuantity;
                        double proratedVolume = ratio * totalSellQuantity;
                        int matchQuantity = 0;

                        if (proratedVolume >= PRO_RATA_MIN_ALLOCATION) {
                            matchQuantity = (int) Math.floor(proratedVolume);
                        }
                        order.setQuantity(order.getQuantity() - matchQuantity);
                        sellOrder.setQuantity(orderProRataList.get(0).getQuantity() - matchQuantity);
                        updateOrder(order);
                        updateOrder(sellOrder);
                        if(matchQuantity > 0){
                            tradeService.createTrade(order, sellOrder, matchQuantity);
                        }
                    }
                    if (sellOrder.getQuantity() <= PRO_RATA_MIN_ALLOCATION) {
                        break;
                    }
                }
            }
        }
    }
    public void proRataBuy(boolean version) {
        List<Order> orderProRataList = getProRataOrder("BUY");

        if (!orderProRataList.isEmpty()) {
            Double bestBuyPrice = getBestBuyPrice();
            Double bestSellPrice = getBestSellPrice();
            if (bestBuyPrice.equals(bestSellPrice)) {
                List<Order> sellOrders = getOrdersAtPrice("SELL", bestSellPrice, version);
                Order buyOrder = orderProRataList.get(0);
                while (buyOrder.getQuantity() > 0) {
                    sellOrders.sort(Comparator.comparing(Order::getQuantity).reversed());
                    double totalBuyQuantity = sellOrders.stream().mapToDouble(Order::getQuantity).sum();
                    double totalSellQuantity = buyOrder.getQuantity();
                    for (Order order : sellOrders) {
                        double ratio = order.getQuantity() / totalBuyQuantity;
                        double proratedVolume = ratio * totalSellQuantity;
                        int matchQuantity = 0;

                        if (proratedVolume >= PRO_RATA_MIN_ALLOCATION) {
                            matchQuantity = (int) Math.floor(proratedVolume);
                        }
                        order.setQuantity(order.getQuantity() - matchQuantity);
                        buyOrder.setQuantity(orderProRataList.get(0).getQuantity() - matchQuantity);
                        updateOrder(order);
                        updateOrder(buyOrder);
                        if(matchQuantity > 0) {
                            tradeService.createTrade(order, buyOrder, matchQuantity);
                        }
                    }
                    if (buyOrder.getQuantity() <= PRO_RATA_MIN_ALLOCATION) {
                        break;
                    }System.out.println("Pro rata Stuck");
                }
            }
        }
    }

    public Map<Double, Long> getTotalOrderAtPrice(String side){
        Pipeline pipeline = Pipeline.create();
        if(side.equals("BUY")){
            pipeline
                    .readFrom(Sources.<String, Order>map("buy-orders"))
                    .groupingKey(entry -> entry.getValue().getPrice())
                    .aggregate(AggregateOperations.counting())
                    .writeTo(Sinks.map("total-order"));
        }
        else{
            pipeline
                    .readFrom(Sources.<String, Order>map("sell-orders"))
                    .groupingKey(entry -> entry.getValue().getPrice())
                    .aggregate(AggregateOperations.counting())
                    .writeTo(Sinks.map("total-order"));
        }
        try{
            jetService.newJob(pipeline).join();
            return new HashMap<>(hazelcastInstance.getMap("total-order"));
        }
        finally {
            hazelcastInstance.getMap("total-order").destroy();
        }
    }

    public Double getBestBuyPrice(){
        IList<Double> buyPriceList = hazelcastInstance.getList("buy-price-list");
        if(!buyPriceList.isEmpty()){
            return buyPriceList.get(buyPriceList.size() -1);
        }
        return 0.0;
    }

    public Double getBestSellPrice(){
        IList<Double> sellPriceList = hazelcastInstance.getList("sell-price-list");
        if(!sellPriceList.isEmpty()){
            return sellPriceList.get(0);
        }
        return 0.0;
    }

    public void updatePriceList(String side, Double price, boolean version2){
        if(version2){
            Map<Double, Long> orderList = getTotalOrderAtPrice(side);
            if (!orderList.containsKey(price)){
                if(side.equals("BUY")){
                    hazelcastInstance.getList("buy-price-list").remove(price);
                }
                else{
                    hazelcastInstance.getList("sell-price-list").remove(price);
                }
            }
        }
        else {
            List<Order> orderList = getOrdersAtPrice(side, price, false);
            if (orderList.isEmpty()){
                if(side.equals("BUY")){
                    hazelcastInstance.getList("buy-price-list").remove(price);
                }
                else{
                    hazelcastInstance.getList("sell-price-list").remove(price);
                }
            }
        }
    }
    public void syncOrderMap(){
        IMap<String, Order> buyMap = hazelcastInstance.getMap("buyMap");
        IMap<String, Order> sellMap = hazelcastInstance.getMap("sellMap");

        IMap<String, Order> buyOrderMap = hazelcastInstance.getMap("buy-orders");
        IMap<String, Order> sellOrderMap = hazelcastInstance.getMap("sell-orders");

        buyOrderMap.putAll(buyMap);
        sellOrderMap.putAll(sellMap);

        buyMap.clear();
        sellMap.clear();
    }
}
