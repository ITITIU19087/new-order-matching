package com.ordermatching.config;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
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
        joinConfig.getMulticastConfig().setEnabled(true).setMulticastPort(5701);
        config.setClusterName("order-matching");
        config.getJetConfig().setEnabled(true).setResourceUploadEnabled(true);
        return config;
    }

    @Bean
    public JetInstance jetInstance(){
        return Jet.newJetInstance(hazelcastConfig().getJetConfig());
    }

    @Bean
    public HazelcastInstance hazelcastInstance(){
        Config config = hazelcastConfig();
        return Hazelcast.newHazelcastInstance(config);
    }
}
