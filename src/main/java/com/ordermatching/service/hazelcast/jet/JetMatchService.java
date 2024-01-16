package com.ordermatching.service.hazelcast.jet;

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.jet.JetInstance;
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
    private JetInstance jetInstance;

    @Autowired
    private JetTradeService tradeService;

    public List<Order> getAllOrders() {
        Pipeline pipeline = Pipeline.create();
        pipeline
                .readFrom(Sources.<String, Order>map("orders1"))
                .filter(entry -> !entry.getValue().isMatched())
                .map(Map.Entry::getValue)
                .writeTo(Sinks.list("list"));
        jetInstance.newJob(pipeline).join();
        return new ArrayList<>(jetInstance.getList("list"));
    }

    public List<Order> getOrdersBySide(String side) {
        Pipeline pipeline = Pipeline.create();
        pipeline
                .readFrom(Sources.<String, Order>map("orders1"))
                .filter(entry -> !entry.getValue().isMatched())
                .filter(entry -> entry.getValue().getSide().equals(side))
                .map(Map.Entry::getValue)
                .writeTo(Sinks.list("list"));
        try {
            jetInstance.newJob(pipeline).join();
            return new ArrayList<>(jetInstance.getList("list"));
        } finally {
            jetInstance.getList("list").destroy();
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
        jetInstance.newJob(pipeline).join();
        return new HashMap<>(jetInstance.getMap("groupedOrdersByPrice"));
    }

    public double getBestBuyPriceJet() {
        groupOrderByPrice("BUY");

        Pipeline pipeline1 = Pipeline.create();
        pipeline1
                .readFrom(Sources.<Double, List<Order>>map("groupedOrdersByPrice"))
                .map(Map.Entry::getKey)
                .writeTo(Sinks.list("bestBuyPrice"));

        jetInstance.newJob(pipeline1).join();

        Pipeline pipeline = Pipeline.create();
        pipeline
                .readFrom(Sources.<Double>list("bestBuyPrice"))
                .aggregate(AggregateOperations.maxBy(Double::compareTo))
                .writeTo(Sinks.list("filtered_list"));
        try {
            jetInstance.newJob(pipeline).join();
            List<Double> list = jetInstance.getList("filtered_list");
            return list.get(list.size()-1);
        } finally {
            jetInstance.getMap("groupedOrdersByPrice").destroy();
            jetInstance.getList("bestBuyPrice").destroy();
        }
    }

    public List<Order> getOrdersAtPrice(String side, Double price) {
        Pipeline pipeline = Pipeline.create();

        pipeline
                .readFrom(Sources.<String, Order>map("orders1"))
                .filter(entry -> entry.getValue().getSide().equals(side))
                .filter(entry -> entry.getValue().getPrice().equals(price))
                .filter(entry -> !entry.getValue().isMatched())
                .map(Map.Entry::getValue)
                .sort(ComparatorEx.comparing(Order::getOrderTime))
                .writeTo(Sinks.list("orderListAtPrice"));

        try {
            jetInstance.newJob(pipeline).join();
            return new ArrayList<>(jetInstance.getList("orderListAtPrice"));
        } finally {
            jetInstance.getList("orderListAtPrice").destroy();
        }
    }

    public void updateOrder(Order order) {
        IMap<String, Order> orderMap = jetInstance.getMap("orders1");
        IMap<String, Order> orderSell = jetInstance.getMap("orders_prorata_sell");
        IMap<String, Order> orderBuy = jetInstance.getMap("orders_prorata_buy");
        orderMap.replace(order.getUUID(), order);
        orderSell.replace(order.getUUID(), order);
        orderBuy.replace(order.getUUID(), order);
        if (order.isMatched()) {
            orderSell.remove(order.getUUID());
            orderBuy.remove(order.getUUID());
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

    public void matchOrdersUsingFifo() {
        double bestBuyPrice = getBestBuyPrice();
        double bestSellPrice = getBestSellPrice();

        while (bestSellPrice == bestBuyPrice) {
            List<Order> buyOrders = getOrdersAtPrice("BUY", bestBuyPrice);
            List<Order> sellOrders = getOrdersAtPrice("SELL", bestSellPrice);

            matchOrders(buyOrders, sellOrders);

            updatePriceList("BUY", bestBuyPrice);
            updatePriceList("SELL", bestSellPrice);

            bestBuyPrice = getBestBuyPrice();
            bestSellPrice = getBestSellPrice();
        }
    }

    public void initialCheck() {
        double bestBuyPrice = getBestBuyPrice();
        double bestSellPrice = getBestSellPrice();

        while (bestSellPrice < bestBuyPrice) {
            List<Order> buyOrders = getOrdersAtPrice("BUY", bestBuyPrice);
            List<Order> sellOrders = getOrdersAtPrice("SELL", bestSellPrice);

            matchOrders(buyOrders, sellOrders);

            updatePriceList("BUY", bestBuyPrice);
            updatePriceList("SELL", bestSellPrice);

            bestBuyPrice = getBestBuyPrice();
            bestSellPrice = getBestSellPrice();
        }
    }
    public List<Order> getProrataOrder(String side) {
        Pipeline pipeline = Pipeline.create();
        try{
            if (side.equals("BUY")) {
                pipeline
                        .readFrom(Sources.<String, Order>map("orders_prorata_buy"))
                        .filter(entry -> !entry.getValue().isMatched())
                        .map(Map.Entry::getValue)
                        .writeTo(Sinks.list("prorata_buy_list"));
                jetInstance.newJob(pipeline).join();
                return new ArrayList<>(jetInstance.getList("prorata_buy_list"));
            } else {
                pipeline
                        .readFrom(Sources.<String, Order>map("orders_prorata_sell"))
                        .filter(entry -> !entry.getValue().isMatched())
                        .map(Map.Entry::getValue)
                        .writeTo(Sinks.list("prorata_sell_list"));
                jetInstance.newJob(pipeline).join();
                return new ArrayList<>(jetInstance.getList("prorata_sell_list"));
            }
        }
         finally {
            jetInstance.getList("prorata_sell_list").destroy();
            jetInstance.getList("prorata_buy_list").destroy();
        }
    }

    public void proRataSell() {
        List<Order> orderProRataList = getProrataOrder("SELL");

        if (!orderProRataList.isEmpty()) {
            Double bestBuyPrice = getBestBuyPrice();
            Double bestSellPrice = getBestSellPrice();
            if (bestBuyPrice.equals(bestSellPrice)) {
                List<Order> buyOrders = getOrdersAtPrice("BUY", bestBuyPrice);
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
                        tradeService.createTrade(order, sellOrder, matchQuantity);
                    }
                    if (sellOrder.getQuantity() <= PRO_RATA_MIN_ALLOCATION) {
                        break;
                    }
                }
            }
        }
    }
    public void proRataBuy() {
        List<Order> orderProRataList = getProrataOrder("BUY");

        if (!orderProRataList.isEmpty()) {
            Double bestBuyPrice = getBestBuyPrice();
            Double bestSellPrice = getBestSellPrice();
            if (bestBuyPrice.equals(bestSellPrice)) {
                List<Order> sellOrders = getOrdersAtPrice("SELL", bestSellPrice);
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
                        tradeService.createTrade(order, buyOrder, matchQuantity);
                    }
                    if (buyOrder.getQuantity() <= PRO_RATA_MIN_ALLOCATION) {
                        break;
                    }
                }
            }
        }
    }

    public Map<Double, Long> getTotalOrderAtPrice(String side){
        Pipeline pipeline = Pipeline.create();
        pipeline
                .readFrom(Sources.<String, Order>map("orders1"))
                .filter(entry -> entry.getValue().getSide().equals(side))
                .filter(entry -> !entry.getValue().isMatched())
                .groupingKey(entry -> entry.getValue().getPrice())
                .aggregate(AggregateOperations.counting())
                .writeTo(Sinks.map("total-order"));
        try{
            jetInstance.newJob(pipeline).join();
            return new HashMap<>(jetInstance.getMap("total-order"));
        }
        finally {
            jetInstance.getMap("total-order").destroy();
        }
    }
    public Double getBestBuyPrice(){
        IList<Double> buyPriceList = jetInstance.getList("buy-price-list");
        return buyPriceList.get(buyPriceList.size() -1);
    }
    public Double getBestSellPrice(){
        IList<Double> sellPriceList = jetInstance.getList("sell-price-list");
        return sellPriceList.get(0);
    }

    public void updatePriceList(String side, Double price){
        List<Order> orderList = getOrdersAtPrice(side, price);

        if (orderList.isEmpty()){
            if(side.equals("BUY")){
                jetInstance.getList("buy-price-list").remove(price);
            }
            else{
                jetInstance.getList("sell-price-list").remove(price);
            }
        }
    }
}
