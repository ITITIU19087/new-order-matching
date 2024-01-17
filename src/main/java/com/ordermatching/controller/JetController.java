package com.ordermatching.controller;

import com.hazelcast.jet.JetInstance;
import com.ordermatching.dto.OrderDto;
import com.ordermatching.entity.Order;
import com.ordermatching.entity.Trade;
import com.ordermatching.entity.TradePrice;
import com.ordermatching.service.hazelcast.TradeService;
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
    private JetInstance jetInstance;

    @Autowired
    private JetMatchService jetMatchService;

    @Autowired
    private JetOrderService jetOrderService;

    @Autowired
    private SocketIOService socketIOService;

    @Autowired
    private JetTradeService tradeService;

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
        return jetInstance.getMap("trades");
    }
}
