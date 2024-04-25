package com.ordermatching.service.hazelcast.newjet;

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;
import com.ordermatching.entity.Order;
import com.ordermatching.service.hazelcast.jet.JetTradeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class NewJetMatchService {

    private static final Double PRO_RATA_MIN_ALLOCATION = 1.0;

    @Autowired
    private HazelcastInstance hazelcastInstance;

    @Autowired
    private JetService jetService;

    @Autowired
    private JetTradeService tradeService;

    public Double getBestPrice(String side){
        if(side.equals("BUY")){
            IList<Double> buyPriceList = hazelcastInstance.getList("buy_price_list");
            if(!buyPriceList.isEmpty()){
                return buyPriceList.get(buyPriceList.size() -1);
            }
            return 0.0;
        }
        else {
            IList<Double> sellPriceList = hazelcastInstance.getList("sell_price_list");
            if(!sellPriceList.isEmpty()){
                return sellPriceList.get(0);
            }
            return 0.0;
        }
    }

    public void syncOrderMaps(){
        IMap<String, Order> evalBuyMap = hazelcastInstance.getMap("eval_buy_orders");
        IMap<String, Order> evalSellMap = hazelcastInstance.getMap("eval_sell_orders");

        IMap<String, Order> buyMap = hazelcastInstance.getMap("waiting_buy_orders");
        IMap<String, Order> sellMap = hazelcastInstance.getMap("waiting_sell_orders");

        IMap<String, Order> newBuyMap = hazelcastInstance.getMap("new_coming_buy_orders");
        IMap<String, Order> newSellMap = hazelcastInstance.getMap("new_coming_sell_orders");

        buyMap.putAll(evalBuyMap);
//        newBuyMap.putAll(evalBuyMap);
        sellMap.putAll(evalSellMap);
//        newSellMap.putAll(evalSellMap);

        evalBuyMap.clear();
        evalSellMap.clear();

    }

    public Map<Double, Long> sumOfOrdersAtPrice(String side){
        Pipeline pipeline = Pipeline.create();
        if(side.equals("BUY")){
            pipeline
                    .readFrom(Sources.<String, Order>map("waiting_buy_orders"))
                    .groupingKey(entry -> entry.getValue().getPrice())
                    .aggregate(AggregateOperations.counting())
                    .writeTo(Sinks.map("sum-order"));
        }
        else{
            pipeline
                    .readFrom(Sources.<String, Order>map("waiting_sell_orders"))
                    .groupingKey(entry -> entry.getValue().getPrice())
                    .aggregate(AggregateOperations.counting())
                    .writeTo(Sinks.map("sum-order"));
        }
        try{
            jetService.newJob(pipeline).join();
            return new HashMap<>(hazelcastInstance.getMap("sum-order"));
        }
        finally {
            hazelcastInstance.getMap("sum-order").destroy();
        }
    }

    public Map<Double, Long> getTotalOrderAtPrice(String side){
        Pipeline pipeline = Pipeline.create();
        if(side.equals("BUY")){
            pipeline
                    .readFrom(Sources.<String, Order>map("waiting_buy_orders"))
                    .groupingKey(entry -> entry.getValue().getPrice())
                    .aggregate(AggregateOperations.counting())
                    .writeTo(Sinks.map("total-order"));
        }
        else{
            pipeline
                    .readFrom(Sources.<String, Order>map("waiting_sell_orders"))
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

    public List<Order> getOrdersAtPrice(String side, Double price, boolean waitingStorage){
        String buyMapName, sellMapName;
        if(waitingStorage){
            buyMapName = "waiting_buy_orders";
            sellMapName = "waiting_sell_orders";
        }
        else{
            buyMapName = "eval_buy_orders";
            sellMapName = "eval_sell_orders";
        }
        Pipeline pipeline = Pipeline.create();
        if(side.equals("BUY")){
            pipeline.readFrom(Sources.<String, Order>map(buyMapName))
                    .filter(entry -> entry.getValue().getPrice().equals(price))
                    .map(Map.Entry::getValue)
                    .sort(ComparatorEx.comparing(Order::getOrderTime))
                    .writeTo(Sinks.list("orderListAtPrice"));
        }
        else {
            pipeline.readFrom(Sources.<String, Order>map(sellMapName))
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

    public void updatePriceList(String side, Double price, boolean waitingMap){
        if(waitingMap){
            Map<Double, Long> priceMap = getTotalOrderAtPrice(side);
            if(!priceMap.containsKey(price)){
                if(side.equals("BUY")){
                    hazelcastInstance.getList("buy_price_list").remove(price);
                    System.out.println("Price case 1: "+ price+ " has been removed from side "+side +"\n");
                }
                else if(side.equals("SELL")){
                    hazelcastInstance.getList("sell_price_list").remove(price);
                    System.out.println("Price case 1: "+ price+ " has been removed from side "+side +"\n");
                }
            }
        }
        else{
            List<Order> orderList = getOrdersAtPrice(side, price, waitingMap);
            if (orderList.isEmpty()){
                if(side.equals("BUY")){
                    hazelcastInstance.getList("buy_price_list").remove(price);
                    System.out.println("Price: "+ price+ " has been removed from side "+side +"\n");
                }
                else if(side.equals("SELL")){
                    hazelcastInstance.getList("sell_price_list").remove(price);
                    System.out.println("Price: "+ price+ " has been removed from side "+side +"\n");
                }
            }
        }
    }

    public void updateOrder(Order order){
        IMap<String, Order> waitingBuyOrder = hazelcastInstance.getMap("waiting_buy_orders");
        IMap<String, Order> waitingSellOrder = hazelcastInstance.getMap("waiting_sell_orders");
        IMap<String, Order> matchedOrder = hazelcastInstance.getMap("matched-orders");
        IMap<String, Order> largeBuyOrders = hazelcastInstance.getMap("large_buy_orders");
        IMap<String, Order> largeSellOrders = hazelcastInstance.getMap("largre_sell_orders");
        IMap<String, Order> evalBuyOrder = hazelcastInstance.getMap("eval_buy_orders");
        IMap<String, Order> evalSellOrder = hazelcastInstance.getMap("eval_sell_orders");
        IMap<String, Order> newBuyOrders = hazelcastInstance.getMap("new_coming_buy_orders");
        IMap<String, Order> newSellOrders = hazelcastInstance.getMap("new_coming_sell_orders");


        waitingBuyOrder.replace(order.getUUID(), order);
        waitingSellOrder.replace(order.getUUID(), order);
        largeBuyOrders.replace(order.getUUID(), order);
        largeSellOrders.replace(order.getUUID(), order);
        evalBuyOrder.replace(order.getUUID(), order);
        evalSellOrder.replace(order.getUUID(), order);
        newBuyOrders.remove(order.getUUID(), order);
        newSellOrders.remove(order.getUUID(),order);

        if(order.isMatched()){
            waitingBuyOrder.remove(order.getUUID());
            waitingSellOrder.remove(order.getUUID());
            largeBuyOrders.remove(order.getUUID());
            largeSellOrders.remove(order.getUUID());
            evalBuyOrder.remove(order.getUUID());
            evalSellOrder.remove(order.getUUID());
            matchedOrder.put(order.getUUID(), order);
        }
    }

    public void executeTrade(Order buyOrder, Order sellOrder){
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

    public void matchOrders(List<Order> buyOrders, List<Order> sellOrders){
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

    public void fifoExe(int executionCase){
        Double bestBuyPrice = getBestPrice("BUY");
        Double bestSellPrice = getBestPrice("SELL");

        if (executionCase == 3){
            if (bestSellPrice.equals(bestBuyPrice)) {
                List<Order> buyOrders = getOrdersAtPrice("BUY", bestBuyPrice, false);
                List<Order> sellOrders = getOrdersAtPrice("SELL", bestSellPrice, false);

                matchOrders(buyOrders, sellOrders);

                updatePriceList("BUY", bestBuyPrice, false);
                updatePriceList("SELL", bestSellPrice, false);
            }
        }
        // take buy orders from waiting map, and sell orders from eval map
        else if( executionCase == 2){
            if (bestSellPrice.equals(bestBuyPrice)) {
                List<Order> buyOrders = getOrdersAtPrice("BUY", bestBuyPrice, true);
                List<Order> sellOrders = getOrdersAtPrice("SELL", bestSellPrice, false);
                System.out.println(sellOrders.get(0));

                matchOrders(buyOrders, sellOrders);

                updatePriceList("BUY", bestBuyPrice, true);
                updatePriceList("SELL", bestSellPrice, false);
            }
        }
        // take buy orders from eval map, sell orders from waiting map
        else if (executionCase == 1) {
            if (bestSellPrice.equals(bestBuyPrice)) {
                List<Order> buyOrders = getOrdersAtPrice("BUY", bestBuyPrice, false);
                List<Order> sellOrders = getOrdersAtPrice("SELL", bestSellPrice, true);

                matchOrders(buyOrders, sellOrders);

                updatePriceList("BUY", bestBuyPrice, false);
                updatePriceList("SELL", bestSellPrice, true);
            }
        }
    }

    public void aggressiveOrderCheck(int executionCase){
        Double bestBuyPrice = getBestPrice("BUY");
        Double bestSellPrice = getBestPrice("SELL");

        if (executionCase == 3){
            while (bestSellPrice < bestBuyPrice) {
                List<Order> buyOrders = getOrdersAtPrice("BUY", bestBuyPrice, false);
                List<Order> sellOrders = getOrdersAtPrice("SELL", bestSellPrice, false);

                matchOrders(buyOrders, sellOrders);

                updatePriceList("BUY", bestBuyPrice, false);
                updatePriceList("SELL", bestSellPrice, false);

                bestBuyPrice = getBestPrice("BUY");
                bestSellPrice = getBestPrice("SELL");
            }
        }
        //Take BUY from waiting
        else if (executionCase == 2){
            while (bestSellPrice < bestBuyPrice) {
                List<Order> buyOrders = getOrdersAtPrice("BUY", bestBuyPrice, true);
                List<Order> sellOrders = getOrdersAtPrice("SELL", bestSellPrice, false);

                matchOrders(buyOrders, sellOrders);

                updatePriceList("BUY", bestBuyPrice, true);
                updatePriceList("SELL", bestSellPrice, false);

                bestBuyPrice = getBestPrice("BUY");
                bestSellPrice = getBestPrice("SELL");
            }
        }
        //Take SELL from waiting
        else if (executionCase == 1){
            while (bestSellPrice < bestBuyPrice) {
                List<Order> buyOrders = getOrdersAtPrice("BUY", bestBuyPrice, false);
                List<Order> sellOrders = getOrdersAtPrice("SELL", bestSellPrice, true);

                matchOrders(buyOrders, sellOrders);

                updatePriceList("BUY", bestBuyPrice, false);
                updatePriceList("SELL", bestSellPrice, true);

                bestBuyPrice = getBestPrice("BUY");
                bestSellPrice = getBestPrice("SELL");
            }
        }
    }

    public Double volumeAtPrice(String side, Double price, boolean isWaiting){
        String buyMap, sellMap;
        if(isWaiting){
            buyMap = "waiting_buy_orders";
            sellMap = "waiting_sell_orders";
        }
        else{
            buyMap = "eval_buy_orders";
            sellMap = "eval_sell_orders";
        }
        Pipeline pipeline = Pipeline.create();
        if(side.equals("BUY")){
            pipeline
                    .readFrom(Sources.<String, Order>map(buyMap))
                    .groupingKey(entry -> entry.getValue().getPrice())
                    .aggregate(AggregateOperations.summingDouble(entry -> entry.getValue().getQuantity()))
                    .writeTo(Sinks.map("volume_map"));
        }
        else{
            pipeline
                    .readFrom(Sources.<String, Order>map(sellMap))
                    .groupingKey(entry -> entry.getValue().getPrice())
                    .aggregate(AggregateOperations.summingDouble(entry -> entry.getValue().getQuantity()))
                    .writeTo(Sinks.map("volume_map"));
        }
//        try{
            jetService.newJob(pipeline).join();
            Map<Double, Double> volumeMap = hazelcastInstance.getMap("volume_map");
            return volumeMap.get(price);
//        }
//        finally {
//            hazelcastInstance.getMap("volume_map").destroy();
//        }
    }

    public List<Order> getProRataOrder(String side) {
        Pipeline pipeline = Pipeline.create();
        try{
            if (side.equals("BUY")) {
                pipeline
                        .readFrom(Sources.<String, Order>map("large_buy_orders"))
                        .filter(entry -> !entry.getValue().isMatched())
                        .map(Map.Entry::getValue)
                        .sort(ComparatorEx.comparing(Order::getPrice).reversed())
                        .writeTo(Sinks.list("prorata_buy_list"));
                jetService.newJob(pipeline).join();
                return new ArrayList<>(hazelcastInstance.getList("prorata_buy_list"));
            } else {
                pipeline
                        .readFrom(Sources.<String, Order>map("large_sell_orders"))
                        .filter(entry -> !entry.getValue().isMatched())
                        .map(Map.Entry::getValue)
                        .sort(ComparatorEx.comparing(Order::getPrice))
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


    public void proRataSellExe(int executionCode){
        boolean isWaitingStorage = false;
        if(executionCode == 2){
            isWaitingStorage = true;
        }
        List<Order> largeSellOrder = getProRataOrder("SELL");// lowest price must be at the start
        Double bestBuyPrice = getBestPrice("BUY");
        if(!largeSellOrder.isEmpty() && bestBuyPrice.equals(largeSellOrder.get(0).getPrice())){
            List<Order> buyOrders = getOrdersAtPrice("BUY", bestBuyPrice, isWaitingStorage);
            Order sellOrder = largeSellOrder.get(0);
            while(sellOrder.getQuantity() > 0){
                List<Integer> matchedList = new ArrayList<>();
                System.out.println("first size: "+ matchedList.size());
                buyOrders.sort(Comparator.comparing(Order::getQuantity).reversed());
                double totalBuyQuantity = buyOrders.stream().mapToDouble(Order::getQuantity).sum();
                double totalSellQuantity = sellOrder.getQuantity();
                for(Order order : buyOrders){
                    double ratio = order.getQuantity() / totalBuyQuantity;
                    double proRatedVolume = ratio * totalSellQuantity;
                    int matchQuantity = 0;

                    if (proRatedVolume >= PRO_RATA_MIN_ALLOCATION) {
                        matchQuantity = (int) Math.floor(proRatedVolume);
                        matchedList.add(matchQuantity);
                    }
                    System.out.println("Matched Quantity: " + matchQuantity);
                    order.setQuantity(order.getQuantity() - matchQuantity);
                    sellOrder.setQuantity(largeSellOrder.get(0).getQuantity() - matchQuantity);
                    updateOrder(order);
                    updateOrder(sellOrder);
                    if(matchQuantity > 0){
                        tradeService.createTrade(order, sellOrder, matchQuantity);
                    }
                }
                System.out.println("MatchedList: " + matchedList.size());
                if(sellOrder.getQuantity() <= PRO_RATA_MIN_ALLOCATION || matchedList.isEmpty()){
                    largeSellOrder.remove(sellOrder);
                    hazelcastInstance.getMap("large_sell_orders").remove(sellOrder.getUUID());
                    break;
                }
            }
        }
    }

    public void proRataBuyExe(int executionCode){
        boolean isWaitingStorage = false;
        if(executionCode == 2){
            isWaitingStorage = true;
        }
        List<Order> largeBuyOrder = getProRataOrder("BUY");
        Double bestSellPrice = getBestPrice("SELL");
        if(!largeBuyOrder.isEmpty() && bestSellPrice.equals(largeBuyOrder.get(0).getPrice())){
            List<Order> sellOrders = getOrdersAtPrice("SELL", bestSellPrice, isWaitingStorage);
            Order buyOrder = largeBuyOrder.get(0);
            while(buyOrder.getQuantity() > 0){
                List<Integer> matchedList = new ArrayList<>();
                System.out.println("first size: "+ matchedList.size());
                sellOrders.sort(Comparator.comparing(Order::getQuantity).reversed());
                double totalBuyQuantity = sellOrders.stream().mapToDouble(Order::getQuantity).sum();
                double totalSellQuantity = buyOrder.getQuantity();
                for(Order order : sellOrders){
                    double ratio = order.getQuantity() / totalBuyQuantity;
                    double proRatedVolume = ratio * totalSellQuantity;
                    int matchQuantity = 0;

                    if (proRatedVolume >= PRO_RATA_MIN_ALLOCATION) {
                        matchQuantity = (int) Math.floor(proRatedVolume);
                        matchedList.add(matchQuantity);
                    }
                    order.setQuantity(order.getQuantity() - matchQuantity);
                    buyOrder.setQuantity(largeBuyOrder.get(0).getQuantity() - matchQuantity);
                    updateOrder(order);
                    updateOrder(buyOrder);
                    if(matchQuantity > 0){
                        tradeService.createTrade(order, buyOrder, matchQuantity);
                    }
                }
                if(buyOrder.getQuantity() <= PRO_RATA_MIN_ALLOCATION || matchedList.isEmpty()){
                    largeBuyOrder.remove(buyOrder);
                    hazelcastInstance.getMap("large_buy_orders").remove(buyOrder.getUUID());
                    break;
                }
            }
        }
    }

    //TODO: Implementations of matching algorithms (Pro Rata), some more tests for FIFO and initCheck, and aggregated data for waiting orders
    //TODO: Pro Rata should be applied for very large order
    //TODO: Aggregated waiting data is too long, need to be fixed (save new data to a new map, aggregated this data then combine with previous data)
}
