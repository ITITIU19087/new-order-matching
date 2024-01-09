package com.ordermatching.controller;

import com.hazelcast.jet.JetInstance;
import com.ordermatching.dto.OrderDto;
import com.ordermatching.entity.Order;
import com.ordermatching.service.hazelcast.jet.JetMatchService;
import com.ordermatching.service.hazelcast.jet.JetOrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@RestController
@RequestMapping("jet")
public class JetController {

    @Autowired
    private JetInstance jetInstance;

    @Autowired
    private JetMatchService jetMatchService;

    @Autowired
    private JetOrderService jetOrderService;

    @GetMapping("/all")
    private List<Order> getAllOrders(){
        return jetMatchService.getAllOrders();
    }

    @GetMapping("/alll")
    private List<Order> getAllOrders(@RequestParam String side){
        return jetMatchService.getOrdersBySide(side);
    }

    @PostMapping("/create")
    public ResponseEntity<String> createOrders(@RequestBody List<OrderDto> orderDtoList){
        jetOrderService.createOrders(orderDtoList);
        return ResponseEntity.ok("Success");
    }

    @GetMapping("/best")
    public Double getBestPrice(@RequestParam String side){
        return jetMatchService.getBestPrice(side);
    }

    @GetMapping("map")
    public Map<Double, List<Order>> mapCheck(){
        return new HashMap<>(jetInstance.getMap("groupedOrdersByPrice"));
    }

    @GetMapping("total")
    public Map<Double, Long> getTotalOrderAtPrice(@RequestParam String side){
        return jetMatchService.getTotalOrderAtPrice(side);
    }
}
