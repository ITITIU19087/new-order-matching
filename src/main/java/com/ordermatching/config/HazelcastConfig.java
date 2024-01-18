package com.ordermatching.config;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.config.MapConfig;
import com.hazelcast.jet.JetService;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableCaching
public class HazelcastConfig {
    public Config hazelcastConfig() {
        Config config = new Config();
        config.setInstanceName("orders");
        config.setClusterName("order-matching");

        MapConfig mapConfig = new MapConfig("buy-orders");
        mapConfig.addIndexConfig(new IndexConfig(IndexType.SORTED, "price"));

        MapConfig mapConfig1 = new MapConfig("sell-orders");
        mapConfig1.addIndexConfig(new IndexConfig(IndexType.SORTED,"price"));

        config.addMapConfig(mapConfig);
        config.addMapConfig(mapConfig1);
        config.getJetConfig().setEnabled(true).setResourceUploadEnabled(true);
        return config;
    }


    public ClientConfig clientConfig() {
        ClientConfig config = new ClientConfig();
        config.setClusterName("dev");
        config.getNetworkConfig().addAddress("127.0.0.1:5701");
        return config;
    }

    @Bean
    public HazelcastInstance hazelcastInstance() {
        Config config = hazelcastConfig();
        return Hazelcast.newHazelcastInstance(config);
    }

    @Bean
    public JetService jetService(){
        return hazelcastInstance().getJet();
    }
}
