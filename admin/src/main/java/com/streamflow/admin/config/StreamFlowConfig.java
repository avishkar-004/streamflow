package com.streamflow.admin.config;

import com.streamflow.client.common.NetworkClient;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for StreamFlow broker connection
 */
@Configuration
@ConfigurationProperties(prefix = "streamflow.broker")
@Data
public class StreamFlowConfig {

    private String host = "localhost";
    private int port = 9092;
    private int connectionTimeout = 5000;
    private int requestTimeout = 10000;

    /**
     * Create NetworkClient bean for communicating with broker
     */
    @Bean
    public NetworkClient networkClient() {
        return new NetworkClient(host, port);
    }
}
