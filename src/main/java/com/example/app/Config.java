package com.example.app;

import java.util.Properties;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class Config {

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrapServers;

    @ConfigProperty(name = "kafka.client.id")
    String clientId;

    @ConfigProperty(name = "kafka.security.protocol")
    String securityProtocol;

    @ConfigProperty(name = "kafka.sasl.mechanism")
    String saslMechanism;

    @ConfigProperty(name = "kafka.sasl.jaas.config")
    String saslJaasConfig;

    @ConfigProperty(name = "kafka.topic")
    String topic;

    @ConfigProperty(name = "azure.storage.connection-string")
    String blobConnectionString;

    @ConfigProperty(name = "azure.storage.container-name")
    String blobContainerName;

    @ConfigProperty(name = "message.payload-size", defaultValue = "512000")
    int payloadSize;

    public Properties kafkaProps() {
        var props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("client.id", clientId);
        props.put("security.protocol", securityProtocol);
        props.put("sasl.mechanism", saslMechanism);
        props.put("sasl.jaas.config", saslJaasConfig);
        return props;
    }

    public String topic() { return topic; }
    public String blobConnectionString() { return blobConnectionString; }
    public String blobContainerName() { return blobContainerName; }
    public int payloadSize() { return payloadSize; }
}
