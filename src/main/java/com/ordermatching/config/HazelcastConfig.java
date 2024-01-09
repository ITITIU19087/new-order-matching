package com.ordermatching.config;


import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.config.MapConfig;
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

    public ClientConfig clientConfig() {
        ClientConfig config = new ClientConfig();
        config.setClusterName("dev");
        config.getNetworkConfig().addAddress("127.0.0.1:5701");
        return config;
    }

    @Bean
    public HazelcastInstance hazelcastInstance(){
        Config config = hazelcastConfig();
        return Hazelcast.newHazelcastInstance(config);
    }
}
