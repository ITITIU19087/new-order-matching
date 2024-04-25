package com.ordermatching.controller;

import com.hazelcast.core.HazelcastInstance;
import com.ordermatching.dto.OrderDto;
import com.ordermatching.entity.Order;
import com.ordermatching.entity.TradePrice;
import com.ordermatching.service.hazelcast.jet.JetTradeService;
import com.ordermatching.service.hazelcast.newjet.NewJetMatchService;
import com.ordermatching.service.hazelcast.newjet.NewJetOrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("jett")
public class TestController {
    @Autowired
    private NewJetOrderService jetOrderService;

    @Autowired
    private NewJetMatchService jetMatchService;

    @Autowired
    private HazelcastInstance hazelcastInstance;

    @Autowired
    private JetTradeService tradeService;

    @PostMapping("create")
    public ResponseEntity<String> createOrders(@RequestBody List<OrderDto> orderDtoList){
        jetOrderService.createOrders(orderDtoList);
        return ResponseEntity.ok("Success");
    }

    @GetMapping("check")
    public List<Double> check(@RequestParam String side){
        return jetOrderService.check(side);
    }

    @GetMapping("all")
    public Object getAll(){
        return hazelcastInstance.getMap("large_buy_orders");
    }

    @GetMapping("volume")
    public Double getVolumeAtPrice(@RequestParam String side, @RequestParam Double price){
        System.out.println(hazelcastInstance.getMap("volume_map"));
        return jetMatchService.volumeAtPrice(side, price, true);
    }
    @GetMapping("group")
    public Map<Double, List<Order>> getOrderList(@RequestParam String side){
        return hazelcastInstance.getMap("grouped_buy_order");
    }

    @GetMapping("price")
    public Double getBestPrice(@RequestParam String side){
        return jetMatchService.getBestPrice(side);
    }

    @GetMapping("price-list")
    public List<Double> getPriceList(@RequestParam String side){
        if(side.equals("BUY")){
            return hazelcastInstance.getList("buy_price_list");
        }
        else{
            return hazelcastInstance.getList("sell_price_list");
        }
    }

    @GetMapping("get")
    public Object getOrderAtPrice(@RequestParam String side, @RequestParam Double price){
        return jetMatchService.getOrdersAtPrice(side, price, true);
    }

    @GetMapping("trade-price")
    public TradePrice getTradePrice(){
        return tradeService.getCandleStickPrice();
    }


    @GetMapping("trade")
    public Object getTrade(){
        return hazelcastInstance.getMap("trades");
    }

    @GetMapping("matched")
    public Object getMatched(){
        return hazelcastInstance.getMap("matched_orders");
    }
}
