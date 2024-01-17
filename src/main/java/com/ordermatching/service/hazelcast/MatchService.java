package com.ordermatching.service.hazelcast;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.map.IMap;
import com.ordermatching.entity.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.*;


@Service
@Slf4j
public class MatchService {

    private static final Double PRO_RATA_MIN = 150.0;
    private static final Double PRO_RATA_MIN_ALLOCATION = 1.0;

    @Autowired
    private JetInstance jetInstance;

    @Autowired
    private TradeService tradeService;

    public List<Order> getAllOrders(){
        IMap<String, Order> orderMap = jetInstance.getMap("orders");
        List<Order> orderList = new ArrayList<>();
        for (Order order : orderMap.values()){
            if (!order.isMatched()){
                orderList.add(order);
            }
        }
        return orderList;
    }

    public List<Order> getAllOrdersBySide(String side){
        IMap<String, Order> orderMap = jetInstance.getMap("orders");
        List<Order> orderListBySide = new ArrayList<>();
        for (Order order: orderMap.values()){
            if(order.getSide().equals(side) && !order.isMatched()){
                orderListBySide.add(order);
            }
        }
        orderListBySide.sort(Comparator.comparing(Order::getOrderTime));
        return orderListBySide;
    }

    public Map<Double, List<Order>> groupOrderByPrice(String side){
        List<Order> allOrderBySide = getAllOrdersBySide(side);
        Map<Double, List<Order>> ordersGroupedByPrice = new HashMap<>();
        for (Order order : allOrderBySide) {
            double price = order.getPrice();
            List<Order> ordersWithSamePrice = ordersGroupedByPrice.getOrDefault(price, new ArrayList<>());
            ordersWithSamePrice.add(order);
            ordersGroupedByPrice.put(price, ordersWithSamePrice);
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
        IMap<String, Order> orderMap = jetInstance.getMap("orders");
        Order oldOrder = orderMap.get(order.getUUID());

        if (oldOrder != null){
            oldOrder.setQuantity(order.getQuantity());
            if (order.getQuantity() == 0) {
                oldOrder.setMatched(order.isMatched());
            }
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

    public List<Double> getPriceAtSide(String side){
        List<Double> priceList = new ArrayList<>(groupOrderByPrice(side).keySet());
        if (side.equals("BUY")){
            Collections.sort(priceList);
        }
        else{
            Collections.sort(priceList, Collections.reverseOrder());
        }
        return priceList;
    }

    public Double getHighestQuantityAtPrice(String side, Double price){
        List<Order> orderList = getOrdersAtPrice(side, price);
        Collections.sort(orderList, Comparator.comparing(Order::getQuantity));
        return orderList.get(0).getQuantity();
    }

    public Order getOrderByQuantity(Double quantity, String side, Double price){
        List<Order> orderList = getOrdersAtPrice(side, price);
        List<Order> filterList = new ArrayList<>();

        for (Order order : orderList){
            if (order.getQuantity().equals(quantity) && !order.isMatched()){
                filterList.add(order);
            }
        }
        return filterList.get(0);
    }

    public Map<Double, Integer> getTotalOrderAtPrice(String side){
        Map<Double, List<Order>> priceMap = groupOrderByPrice(side);
        Map<Double, Integer> priceCountMap = new HashMap<>();

        for (Double orderPrice : priceMap.keySet()) {
            List<Order> orderList = priceMap.get(orderPrice);
            int orderCount = orderList.size();
            priceCountMap.put(orderPrice, orderCount);
        }
        return priceCountMap;
    }

    public void initialCheck(){
        double bestBuyPrice = getBestPriceOfSide("BUY");
        log.info("BEST BUY PRICE: "+ bestBuyPrice);
        double bestSellPrice = getBestPriceOfSide("SELL");
        log.info("BEST SELL PRICE: "+ bestSellPrice);

        List<Order> buyOrders = getOrdersAtPrice("BUY", bestBuyPrice);
        List<Order> sellOrders = getOrdersAtPrice("SELL", bestSellPrice);
        while (bestSellPrice < bestBuyPrice){
            log.info("initCheck");
            log.info("BEST BUY PRICE: "+ bestBuyPrice);
            log.info("BEST SELL PRICE: "+ bestSellPrice);
            xxMatch(buyOrders, sellOrders);
            buyOrders = getOrdersAtPrice("BUY", getBestPriceOfSide("BUY"));
            sellOrders = getOrdersAtPrice("SELL", getBestPriceOfSide("SELL"));

            bestBuyPrice = getBestPriceOfSide("BUY");
            bestSellPrice = getBestPriceOfSide("SELL");
        }
    }

    public void matchOrdersUsingFifo(){
        double bestBuyPrice = getBestPriceOfSide("BUY");
        double bestSellPrice = getBestPriceOfSide("SELL");

        List<Order> buyOrders = getOrdersAtPrice("BUY", bestBuyPrice);
        List<Order> sellOrders = getOrdersAtPrice("SELL", bestSellPrice);
        xxMatch(buyOrders, sellOrders);
    }
    public void proRataBuy(){
        Double bestBuyPrice = getBestPriceOfSide("BUY");
        Double bestSellPrice = getBestPriceOfSide("SELL");
        Double highestBuyQuantity = getHighestQuantityAtPrice("BUY", bestBuyPrice);

        if (highestBuyQuantity >= PRO_RATA_MIN && bestBuyPrice.equals(bestSellPrice)){
            Order buyOrder = getOrderByQuantity(highestBuyQuantity, "BUY", bestBuyPrice);
            while (buyOrder.getQuantity() > 0){
                List<Order> sellOrders = getOrdersAtPrice("SELL", bestSellPrice);
                sellOrders.sort(Comparator.comparing(Order::getQuantity).reversed());
                double totalSellQuantity = sellOrders.stream().mapToDouble(Order::getQuantity).sum();
                double totalBuyQuantity = buyOrder.getQuantity();
                for(Order order : sellOrders){
                    double ratio = order.getQuantity() / totalSellQuantity;
                    double proratedVolume = ratio * totalBuyQuantity;
                    log.info("prorated: " + proratedVolume);
                    int matchQuantity = 0;

                    if (proratedVolume >= PRO_RATA_MIN_ALLOCATION){
                        matchQuantity = (int) Math.floor(proratedVolume);
                    }
                    order.setQuantity(order.getQuantity() - matchQuantity);
                    log.info( "SELL order: " + order.getQuantity());
                    log.info("matchedQuantity: " + matchQuantity);
                    log.info( "BUY order: " +order.getQuantity());
                    buyOrder.setQuantity(buyOrder.getQuantity() - matchQuantity);
                    updateOrder(order);
                    updateOrder(buyOrder);
                }

                if (buyOrder.getQuantity() <= PRO_RATA_MIN_ALLOCATION){
                    break;
                }
            }
        }
    }

    public void proRataSell() {
        Double bestBuyPrice = getBestPriceOfSide("BUY");
        Double bestSellPrice = getBestPriceOfSide("SELL");
        Double highestSellQuantity = getHighestQuantityAtPrice("SELL", bestSellPrice);

        if (highestSellQuantity >= PRO_RATA_MIN && bestBuyPrice.equals(bestSellPrice)){
            Order sellOrder = getOrderByQuantity(highestSellQuantity, "SELL", bestSellPrice);
            while (sellOrder.getQuantity() > 0){
                List<Order> buyOrders = getOrdersAtPrice("BUY", bestBuyPrice);
                buyOrders.sort(Comparator.comparing(Order::getQuantity).reversed());
                double totalBuyQuantity = buyOrders.stream().mapToDouble(Order::getQuantity).sum();
                double totalSellQuantity = sellOrder.getQuantity();
                for(Order order : buyOrders){
                    double ratio = order.getQuantity() / totalBuyQuantity;
                    double proratedVolume = ratio * totalSellQuantity;
                    log.info("prorated: " + proratedVolume);
                    int matchQuantity = 0;

                    if (proratedVolume >= PRO_RATA_MIN_ALLOCATION){
                        matchQuantity = (int) Math.floor(proratedVolume);
                    }
                    order.setQuantity(order.getQuantity() - matchQuantity);
                    log.info( "BUY order: " + order.getQuantity());
                    log.info("matchedQuantity: " + matchQuantity);
                    sellOrder.setQuantity(sellOrder.getQuantity() - matchQuantity);
                    updateOrder(order);
                    updateOrder(sellOrder);
                }
                log.info("SELL order: "+sellOrder.getQuantity());
                if (sellOrder.getQuantity() <= PRO_RATA_MIN_ALLOCATION){
                    break;
                }
            }
        }
    }

    public void specialExecuteTrade(Order buyOrder, Order sellOrder){
        if (buyOrder.getPrice() > sellOrder.getPrice()){
            buyOrder.setPrice(sellOrder.getPrice());
        }
        else if (buyOrder.getPrice() < sellOrder.getPrice()){
            sellOrder.setPrice(buyOrder.getPrice());
        }
        executeTrade(buyOrder, sellOrder);
    }

    private void xxMatch(List<Order> buyOrders, List<Order> sellOrders) {
        while (buyOrders.iterator().hasNext() && sellOrders.iterator().hasNext()){
            Order buyOrder = buyOrders.iterator().next();
            Order sellOrder = sellOrders.iterator().next();
            if (buyOrder.getPrice().equals(sellOrder.getPrice())){
                executeTrade(buyOrder, sellOrder);
            }
            else {
                executeTrade(buyOrder, sellOrder);
            }

            if(buyOrder.isMatched()){
                buyOrders.remove(buyOrder);
            }
            if(sellOrder.isMatched()){
                sellOrders.remove(sellOrder);
            }
        }
    }
}
