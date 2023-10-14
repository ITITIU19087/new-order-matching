package com.ordermatching.controller;

import com.ordermatching.dto.OrderDto;
import com.ordermatching.entity.Order;
import com.ordermatching.service.hazelcast.OrderService;
import com.ordermatching.service.hazelcast.MatchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("hazelcast")
public class OrderController {
    @Autowired
    private MatchService matchService;
    @Autowired
    private OrderService orderService;

    @PostMapping("/create")
    public void createOrder(@RequestBody List<OrderDto> orderDtoList){
        for (OrderDto orderDto: orderDtoList) {
            orderService.createOrder(orderDto);
        }
    }

    @GetMapping("group")
    public Map<Double, List<Order>> getOrderByPriceAndSide(@RequestParam String side){
        return matchService.groupOrderByPrice(side);
    }

    @GetMapping("best-price")
    public Double getBestPriceOfSide (@RequestParam String side){
        return matchService.getBestPriceOfSide(side);
    }

    @GetMapping("group-order")
    public List<Order> getAllOrderAtPrice(@RequestParam String side){
        return matchService.getOrdersAtPrice(side, matchService.getBestPriceOfSide(side));
    }
}
