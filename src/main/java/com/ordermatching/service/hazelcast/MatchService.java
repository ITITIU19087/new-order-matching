package com.ordermatching.service.hazelcast;

import com.hazelcast.map.IMap;
import com.ordermatching.config.HazelcastConfig;
import com.ordermatching.entity.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class MatchService {
    @Autowired
    private HazelcastConfig hazelcastConfig;

    @Autowired
    private TradeService tradeService;

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
        List<Order> allOrderBySide = getAllOrdersBySide(side);
        Map<Double, List<Order>> ordersGroupedByPrice = new HashMap<>();

        for (Order order : allOrderBySide) {
            if (!order.isMatched()) {
                double price = order.getPrice();
                List<Order> ordersWithSamePrice = ordersGroupedByPrice.getOrDefault(price, new ArrayList<>());
                ordersWithSamePrice.add(order);
                ordersGroupedByPrice.put(price, ordersWithSamePrice);
            }
        }
        return ordersGroupedByPrice;
    }

    public Double getBestPriceOfSide(String side){
        Map<Double, List<Order>> priceMap = groupOrderByPrice(side);
        if (priceMap.isEmpty()){
            return 0.0;
        }
        if (side.equals("BUY")){
            return Collections.max(priceMap.keySet());
        }
        else{
            return Collections.min(priceMap.keySet());
        }
    }

    public List<Order> getOrdersAtPrice(String side, Double price){
        Map<Double, List<Order>> priceMap = groupOrderByPrice(side);
        for (Double orderPrice : priceMap.keySet()){
            if (orderPrice.equals(price)){
                List<Order> orderList = priceMap.get(orderPrice);
                orderList.sort(Comparator.comparing(Order::getOrderTime));
                return orderList;
            }
        }
        return Collections.emptyList();
    }

    public void updateOrder(Order order){
        IMap<String, Order> orderMap = hazelcastConfig.hazelcastInstance().getMap("orders");
        Order oldOrder = orderMap.get(order.getUUID());

        if (oldOrder != null){
            oldOrder.setQuantity(order.getQuantity());
            oldOrder.setMatched(order.isMatched());
            orderMap.put(order.getUUID(), oldOrder);
        }
    }
    public void executeTrade(Order buyOrder, Order sellOrder){
        Double buyQuantity = buyOrder.getQuantity();
        Double sellQuantity = sellOrder.getQuantity();
        if (buyQuantity - sellQuantity == 0){
            buyOrder.setMatched(true);
            buyOrder.setQuantity(0.0);

            sellOrder.setMatched(true);
            sellOrder.setQuantity(0.0);

            tradeService.createTrade(buyOrder, sellOrder, buyQuantity);
            updateOrder(buyOrder);
            updateOrder(sellOrder);
        } else if (buyQuantity - sellQuantity > 0) {
            buyOrder.setQuantity(buyQuantity - sellQuantity);

            sellOrder.setMatched(true);
            sellOrder.setQuantity(0.0);

            tradeService.createTrade(buyOrder, sellOrder, sellQuantity);
            updateOrder(buyOrder);
            updateOrder(sellOrder);
        }
        else {
            buyOrder.setMatched(true);
            buyOrder.setQuantity(0.0);

            sellOrder.setQuantity(sellQuantity - buyQuantity);

            tradeService.createTrade(buyOrder, sellOrder, buyQuantity);
            updateOrder(buyOrder);
            updateOrder(sellOrder);
        }
    }
    public void matchOrdersUsingFifo(){
        Double bestBuyPrice = getBestPriceOfSide("BUY");
        Double bestSellPrice = getBestPriceOfSide("SELL");
        if (bestBuyPrice.equals(bestSellPrice)){
            List<Order> buyOrders = getOrdersAtPrice("BUY", bestBuyPrice);
            List<Order> sellOrders = getOrdersAtPrice("SELL", bestSellPrice);
            while (buyOrders.iterator().hasNext() && sellOrders.iterator().hasNext()){
                Order buyOrder = buyOrders.iterator().next();
                Order sellOrder = sellOrders.iterator().next();
                executeTrade(buyOrder, sellOrder);

                if(buyOrder.isMatched()){
                    buyOrders.remove(buyOrder);
                }
                if(sellOrder.isMatched()){
                    sellOrders.remove(sellOrder);
                }
            }
        }
    }
}
