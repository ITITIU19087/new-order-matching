package com.ordermatching.controller;

import com.ordermatching.dto.OrderDto;
import com.ordermatching.entity.Order;
import com.ordermatching.service.hazelcast.jet.JetMatchService;
import com.ordermatching.service.hazelcast.jet.JetOrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.ListResourceBundle;
import java.util.Map;

@RestController
@RequestMapping("jet")
public class JetController {
    @Autowired
    private JetMatchService jetMatchService;

    @Autowired
    private JetOrderService jetOrderService;

    @GetMapping("/all")
    private List<Order> getAllOrders(){
        return jetMatchService.getAllOrders();
    }

    @PostMapping("/create")
    public ResponseEntity<String> createOrders(@RequestBody List<OrderDto> orderDtoList){
        jetOrderService.createOrders(orderDtoList);
        return ResponseEntity.ok("Success");
    }

    @PostMapping("/get-by-side")
    public List<Order> getOrderBySide(@RequestParam String side){
        return jetMatchService.getAllOrderBySide(side);
    }

    @PostMapping("/group")
    public Map<Double, List<Order>> groupOrderByPrice(@RequestParam String side){
        return jetMatchService.groupOrderByPrice(side);
    }
}
