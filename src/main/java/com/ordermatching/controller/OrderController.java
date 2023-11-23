package com.ordermatching.controller;

import com.ordermatching.dto.OrderDto;
import com.ordermatching.entity.Order;
import com.ordermatching.entity.Trade;
import com.ordermatching.entity.TradePrice;
import com.ordermatching.service.hazelcast.OrderService;
import com.ordermatching.service.hazelcast.MatchService;
import com.ordermatching.service.hazelcast.TradeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("hazelcast")
@CrossOrigin(origins = "http://localhost:3000")
public class OrderController {
    @Autowired
    private MatchService matchService;

    @Autowired
    private OrderService orderService;

    @Autowired
    private TradeService tradeService;

    @PostMapping("/create")
    public void createOrder(@RequestBody List<OrderDto> orderDtoList){
        orderService.createOrder(orderDtoList);
    }

    @GetMapping("group")
    public Map<Double, List<Order>> getOrderByPriceAndSide(@RequestParam String side){
        return matchService.groupOrderByPrice(side);
    }

    @GetMapping("/service")
    public List<Order> listOrder(){
        return matchService.getAllOrders();
    }

    @GetMapping("best-price")
    public Double getBestPriceOfSide (@RequestParam String side){
        return matchService.getBestPriceOfSide(side);
    }

    @GetMapping("group-order")
    public List<Order> getAllOrderAtPrice(@RequestParam String side){
        return matchService.getAllOrdersBySide(side);
    }

    @GetMapping("get-price")
    public List<Double> getPriceAtSide(@RequestParam String side){
        return matchService.getPriceAtSide(side);
    }


    @GetMapping("total")
    public Map<Double, Integer> getTotalOrderAtPrice(@RequestParam String side){
        return matchService.getTotalOrderAtPrice(side);
    }

    @GetMapping("trade-price")
    public List<Trade> getTrade(){
        return tradeService.getCandlePrice();
    }

    @GetMapping("candle-price")
    public TradePrice getTradePrice(){
        return tradeService.getCandleStickPrice();
    }
}
