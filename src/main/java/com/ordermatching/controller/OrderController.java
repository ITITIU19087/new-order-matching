package com.ordermatching.controller;

import com.ordermatching.dto.OrderDto;
import com.ordermatching.service.hazelcast.OrderService;
import com.ordermatching.service.hazelcast.TradeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("hazelcast")
public class OrderController {
    @Autowired
    private TradeService tradeService;
    @Autowired
    private OrderService orderService;

    @PostMapping("/create")
    public void createOrder(@RequestBody List<OrderDto> orderDtoList){
        for (OrderDto orderDto: orderDtoList) {
            orderService.createOrder(orderDto);
        }
    }
}
