package com.example.app;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import reactor.core.publisher.Mono;

@ApplicationScoped
public class BlobStorage {
    
    @Inject
    Config config;
    
    private BlobContainerAsyncClient container;

    @PostConstruct
    void init() {
        this.container = new BlobServiceClientBuilder()
            .connectionString(config.blobConnectionString())
            .buildAsyncClient()
            .getBlobContainerAsyncClient(config.blobContainerName());
        
        container.exists()
            .flatMap(exists -> exists ? Mono.empty() : container.create())
            .block();
    }

    public String upload(String id, String payload) {
        return uploadAsync(id, payload).block();
    }

    public Mono<String> uploadAsync(String id, String payload) {
        var blobClient = container.getBlobAsyncClient(id + ".txt");
        return blobClient.upload(BinaryData.fromString(payload))
            .then(Mono.just(blobClient.getBlobUrl()));
    }
}
