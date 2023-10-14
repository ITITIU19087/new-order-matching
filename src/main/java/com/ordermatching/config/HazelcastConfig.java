package com.ordermatching.config;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableCaching
public class HazelcastConfig {
    public Config hazelcastConfig(){
        Config config = new Config();
        config.setInstanceName("orders");
        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.getMulticastConfig().setEnabled(true);
        config.setClusterName("order-matching");
        config.getJetConfig().setEnabled(true);
        return config;
    }

    @Bean
    public HazelcastInstance hazelcastInstance(){
        Config config = hazelcastConfig();
        return Hazelcast.newHazelcastInstance(config);
    }
}
