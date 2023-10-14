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
    public void executeTrade(Order buyOrder, Order sellOrder){
        Double matchedQuantity = buyOrder.getQuantity() - sellOrder.getQuantity();
        if (matchedQuantity == 0){
            buyOrder.setMatched(true);
            buyOrder.setQuantity(0.0);

            sellOrder.setMatched(true);
            sellOrder.setQuantity(0.0);

            tradeService.createTrade(buyOrder, sellOrder, matchedQuantity);
        } else if (matchedQuantity > 0) {
            buyOrder.setQuantity(matchedQuantity);

            sellOrder.setMatched(true);
            sellOrder.setQuantity(0.0);

            tradeService.createTrade(buyOrder, sellOrder, matchedQuantity);
        }
        else {
            buyOrder.setMatched(true);
            buyOrder.setQuantity(0.0);

            sellOrder.setQuantity(Math.abs(matchedQuantity));

            tradeService.createTrade(buyOrder, sellOrder, Math.abs(matchedQuantity));
        }
    }

}
