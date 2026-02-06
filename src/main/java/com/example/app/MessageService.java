package com.example.app;

import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jboss.logging.Logger;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import io.quarkus.runtime.Startup;
import reactor.core.publisher.Flux;

@Startup
@ApplicationScoped
public class MessageService {

    private static final Logger LOG = Logger.getLogger(MessageService.class);

    @Inject
    Config config;

    @Inject
    BlobStorage blobStorage;

    private MiniCluster miniCluster;
    private KafkaSink<String> kafkaSink;
    private String restAddress;
    private int restPort;

    @PostConstruct
    void init() {
        try {
            LOG.info("Starting Flink MiniCluster...");
            var flinkConfig = new Configuration();
            flinkConfig.set(RestOptions.BIND_PORT, "0");  // Random available port
            
            var clusterConfig = new MiniClusterConfiguration.Builder()
                .setNumTaskManagers(1)
                .setNumSlotsPerTaskManager(2)
                .setConfiguration(flinkConfig)
                .build();
            
            miniCluster = new MiniCluster(clusterConfig);
            miniCluster.start();
            
            var restUri = miniCluster.getRestAddress().get();
            restAddress = restUri.getHost();
            restPort = restUri.getPort();
            
            // Build reusable KafkaSink
            var sinkBuilder = KafkaSink.<String>builder()
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                    .setTopic(config.topic())
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build());
            config.kafkaProps().stringPropertyNames().forEach(k -> 
                sinkBuilder.setProperty(k, config.kafkaProps().getProperty(k)));
            kafkaSink = sinkBuilder.build();
            
            LOG.infof("Flink MiniCluster started at %s:%d", restAddress, restPort);
        } catch (Exception e) {
            LOG.error("Failed to start Flink MiniCluster", e);
            throw new RuntimeException(e);
        }
    }

    @PreDestroy
    void shutdown() {
        if (miniCluster != null) {
            try {
                LOG.info("Shutting down Flink MiniCluster...");
                miniCluster.close();
                LOG.info("Flink MiniCluster stopped");
            } catch (Exception e) {
                LOG.error("Error shutting down MiniCluster", e);
            }
        }
    }

    public long sendMessages(int messageCount, boolean externalPayload, boolean useFlink, int payloadSize) throws Exception {
        var payload = generatePayload(payloadSize);
        
        LOG.infof("Sending %d messages (%s payload) to '%s' using %s",
            messageCount, externalPayload ? "external" : "inline",
            config.topic(), useFlink ? "Flink" : "Kafka Producer");
        
        var duration = useFlink
            ? sendWithFlink(messageCount, externalPayload, payload)
            : sendWithKafka(messageCount, externalPayload, payload);
        
        LOG.infof("Sent %d messages in %dms (%.2f msg/sec)",
            messageCount, duration, messageCount * 1000.0 / duration);
        
        return duration;
    }

    private long sendWithKafka(int messageCount, boolean externalPayload, String payload) throws Exception {
        var start = System.currentTimeMillis();
        var props = new Properties();
        props.putAll(config.kafkaProps());
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        
        try (var producer = new KafkaProducer<String, String>(props)) {
            for (int i = 0; i < messageCount; i++) {
                var msgStart = System.currentTimeMillis();
                var id = UUID.randomUUID().toString();
                var msgPayload = externalPayload ? blobStorage.upload(id, payload) : payload;
                
                producer.send(new ProducerRecord<>(config.topic(), buildMessage(id, msgPayload))).get();
                LOG.infof("[%d/%d] id=%s time=%dms", i + 1, messageCount, id, System.currentTimeMillis() - msgStart);
            }
        }
        return System.currentTimeMillis() - start;
    }

    private long sendWithFlink(int messageCount, boolean externalPayload, String payload) throws Exception {
        var totalStart = System.currentTimeMillis();
        LOG.info("Preparing messages...");
        
        List<String> messages;
        if (externalPayload) {
            var uploadStart = System.currentTimeMillis();
            messages = Flux.range(0, messageCount)
                .map(i -> UUID.randomUUID().toString())
                .flatMap(id -> blobStorage.uploadAsync(id, payload).map(url -> buildMessage(id, url)))
                .collectList().block();
            LOG.infof("Uploaded %d blobs in %dms (async)", messageCount, System.currentTimeMillis() - uploadStart);
        } else {
            messages = IntStream.range(0, messageCount)
                .mapToObj(i -> buildMessage(UUID.randomUUID().toString(), payload))
                .toList();
        }
        
        LOG.info("Sending batch via Flink...");
        var sendStart = System.currentTimeMillis();
        
        var env = StreamExecutionEnvironment.createRemoteEnvironment(restAddress, restPort);
        env.fromCollection(messages).sinkTo(kafkaSink);
        env.execute("Send to EventHub");
        
        LOG.infof("Flink send time: %dms", System.currentTimeMillis() - sendStart);
        return System.currentTimeMillis() - totalStart;
    }

    private String generatePayload(int sizeBytes) {
        var bytes = new byte[(int) (sizeBytes * 0.75)];
        ThreadLocalRandom.current().nextBytes(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }

    private String buildMessage(String id, String payload) {
        return "{\"id\":\"" + id + "\",\"payload\":\"" + payload + "\"}";
    }
}
