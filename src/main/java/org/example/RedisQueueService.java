package org.example;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.UUID;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Redis-backed QueueService implementation using Upstash REST API.
 */
public class RedisQueueService implements QueueService {

    private static final String UPSTASH_URL = System.getenv("UPSTASH_REDIS_REST_URL");
    private static final String TOKEN = System.getenv("UPSTASH_REDIS_REST_TOKEN");

    private static final long VISIBILITY_TIMEOUT_MS = 30_000; // 30 seconds

    private final HttpClient client;
    private final ObjectMapper objectMapper;

    /** Default constructor (production) */
    public RedisQueueService() {
        this(HttpClient.newHttpClient());
    }

    /** Constructor for injecting custom HttpClient (useful for tests) */
    public RedisQueueService(HttpClient client) {
        this.client = client;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void push(String queueUrl, String messageBody) {
        try {
            Message message = new Message(messageBody, 0); // default priority 0
            String payload = objectMapper.writeValueAsString(message);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(UPSTASH_URL + "/lpush/" + queueUrl))
                    .header("Authorization", "Bearer " + TOKEN)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("[\"" + payload + "\"]")) // LPUSH expects an array
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new RuntimeException("Failed to push message: " + response.body());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Message pull(String queueUrl) {
        try {
            // Pop from Redis list
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(UPSTASH_URL + "/rpop/" + queueUrl))
                    .header("Authorization", "Bearer " + TOKEN)
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200 || response.body().equals("null")) {
                return null;
            }

            // Deserialize message
            Message message = objectMapper.readValue(response.body(), Message.class);
            message.incrementAttempts();
            message.setReceiptId(UUID.randomUUID().toString());
            message.setVisibleFrom(System.currentTimeMillis() + VISIBILITY_TIMEOUT_MS);

            // Store in inflight map for delete()
            String tempPayload = objectMapper.writeValueAsString(message);
            HttpRequest storeRequest = HttpRequest.newBuilder()
                    .uri(URI.create(UPSTASH_URL + "/hset/inflight " + message.getReceiptId() + " '" + tempPayload + "'"))
                    .header("Authorization", "Bearer " + TOKEN)
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build();
            client.send(storeRequest, HttpResponse.BodyHandlers.ofString());

            return message;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(String queueUrl, String receiptId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(UPSTASH_URL + "/hdel/inflight " + receiptId))
                    .header("Authorization", "Bearer " + TOKEN)
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build();

            client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    void requeueExpiredMessages() {
        try {
            // Get all keys in inflight
            HttpRequest keysRequest = HttpRequest.newBuilder()
                    .uri(URI.create(UPSTASH_URL + "/hkeys/inflight"))
                    .header("Authorization", "Bearer " + TOKEN)
                    .GET()
                    .build();

            HttpResponse<String> keysResponse = client.send(keysRequest, HttpResponse.BodyHandlers.ofString());
            if (keysResponse.statusCode() != 200 || keysResponse.body().equals("null")) return;

            List<String> keys = objectMapper.readValue(keysResponse.body(), List.class);
            for (String receiptId : keys) {
                // Fetch message
                HttpRequest getRequest = HttpRequest.newBuilder()
                        .uri(URI.create(UPSTASH_URL + "/hget/inflight/" + receiptId))
                        .header("Authorization", "Bearer " + TOKEN)
                        .GET()
                        .build();
                HttpResponse<String> getResponse = client.send(getRequest, HttpResponse.BodyHandlers.ofString());
                if (getResponse.statusCode() != 200 || getResponse.body().equals("null")) continue;

                Message message = objectMapper.readValue(getResponse.body(), Message.class);
                if (message.isVisibleAt(System.currentTimeMillis())) {
                    // Requeue message
                    push("default", message.getBody());
                    // Remove from inflight
                    delete("default", receiptId);
                }
            }
        } catch (Exception e) {
            e.printStackTrace(); // log error but continue
        }
    }
}
