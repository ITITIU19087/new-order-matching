package com.ordermatching.controller;

import com.hazelcast.collection.IList;
import com.ordermatching.service.hazelcast.jet.JetMatchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("jet")
public class JetController {
    @Autowired
    private JetMatchService jetMatchService;

    @GetMapping("/all")
    private List<Object> getAllOrders(){
        return jetMatchService.getAllOrders();
    }
}
