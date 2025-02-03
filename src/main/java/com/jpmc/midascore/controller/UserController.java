package com.jpmc.midascore.controller;

import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
public class UserController {
    
    @Autowired
    private KafkaConsumer kafkaConsumer;

    @GetMapping("/balance")
    public Balance getBalance(@RequestParam Long userId){
        return kafkaConsumer.getBalance(userId);
    }

}
