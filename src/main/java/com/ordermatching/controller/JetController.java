package com.ordermatching.controller;

import com.hazelcast.core.HazelcastInstance;
import com.ordermatching.dto.OrderDto;
import com.ordermatching.entity.Order;
import com.ordermatching.entity.Trade;
import com.ordermatching.entity.TradePrice;
import com.ordermatching.service.hazelcast.jet.JetMatchService;
import com.ordermatching.service.hazelcast.jet.JetOrderService;
import com.ordermatching.service.hazelcast.jet.JetTradeService;
import com.ordermatching.service.socketio.SocketIOService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("jet")
@CrossOrigin(origins = "http://localhost:3000")
public class JetController {

    @Autowired
    private HazelcastInstance hazelcastInstance;

    @Autowired
    private JetMatchService jetMatchService;

    @Autowired
    private JetOrderService jetOrderService;

    @Autowired
    private SocketIOService socketIOService;

    @Autowired
    private JetTradeService tradeService;

    @GetMapping("/alll")
    private List<Order> getAllOrders(@RequestParam String side){
        return jetMatchService.getOrdersBySide(side);
    }

    @PostMapping("/create")
    public ResponseEntity<String> createOrders(@RequestBody List<OrderDto> orderDtoList){
        jetOrderService.createOrders(orderDtoList);
        return ResponseEntity.ok("Success");
    }

    @GetMapping("map")
    public Map<Double, List<Order>> mapCheck(@RequestParam String side){
        return jetMatchService.groupOrderByPrice(side);
    }

    @GetMapping("total")
    public String getTotalOrderAtPrice(@RequestParam String side){
        return jetMatchService.getTotalOrderAtPrice(side).toString();
    }
    @GetMapping("total-price")
    public Map<Double, Long> getAllOrderAtPrice(@RequestParam String side){
        return jetMatchService.getTotalOrderAtPrice(side);
    }

    @GetMapping("socket")
    public void socketDumps(){
        socketIOService.dumps("Dumps msg");
    }

    @GetMapping("trade")
    public TradePrice getTrades(){
        return tradeService.getCandleStickPrice();
    }

    @GetMapping("trades")
    public Map<String, Trade> getTrade(){
        return hazelcastInstance.getMap("trades");
    }

    @GetMapping("best-price")
    public Double getBestPrice(@RequestParam String side){
        if (side.equals("BUY")){
            return jetMatchService.getBestBuyPrice();
        }
        else return jetMatchService.getBestSellPrice();
    }

    @GetMapping("order")
    public List<Order> getOrderListAtPrice(@RequestParam String side, @RequestParam Double price, @RequestParam boolean version){
        return jetMatchService.getOrdersAtPrice(side, price, version);
    }

}
