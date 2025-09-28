package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RedisQueueServiceTest {

    private HttpClient client;
    private RedisQueueService service;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        client = mock(HttpClient.class);
        service = new RedisQueueService(client);
        objectMapper = new ObjectMapper();
    }

    @Test
    void testPushSuccess() throws Exception {
        HttpResponse<String> mockResponse = mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(200);
        when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

        assertDoesNotThrow(() -> service.push("myQueue", "hello"));
    }

    @Test
    void testPullReturnsMessage() throws Exception {
        Message msg = new Message("hello", 0);
        String msgJson = objectMapper.writeValueAsString(msg);

        HttpResponse<String> rpopResponse = mock(HttpResponse.class);
        when(rpopResponse.statusCode()).thenReturn(200);
        when(rpopResponse.body()).thenReturn(msgJson);

        HttpResponse<String> inflightResponse = mock(HttpResponse.class);
        when(inflightResponse.statusCode()).thenReturn(200);

        when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(rpopResponse)      // first call = RPOP
                .thenReturn(inflightResponse); // second call = HSET inflight

        Message pulled = service.pull("myQueue");

        assertNotNull(pulled);
        assertEquals("hello", pulled.getBody());
        assertNotNull(pulled.getReceiptId());
    }

    @Test
    void testPullEmptyQueue() throws Exception {
        HttpResponse<String> mockResponse = mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn("null");

        when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

        Message pulled = service.pull("myQueue");
        assertNull(pulled);
    }

    @Test
    void testDeleteSuccess() throws Exception {
        HttpResponse<String> mockResponse = mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(200);

        when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

        assertDoesNotThrow(() -> service.delete("myQueue", "receipt123"));
    }

    @Test
    void testRequeueExpiredMessage() throws Exception {
        // Prepare a message in inflight that is already expired
        Message msg = new Message("hello", 0);
        msg.setReceiptId("receipt123");
        msg.setVisibleFrom(System.currentTimeMillis() - 1000); // expired

        String msgJson = objectMapper.writeValueAsString(msg);

        // Mock inflight keys
        HttpResponse<String> keysResponse = mock(HttpResponse.class);
        when(keysResponse.statusCode()).thenReturn(200);
        when(keysResponse.body()).thenReturn(objectMapper.writeValueAsString(List.of("receipt123")));

        // Mock HGET inflight
        HttpResponse<String> hgetResponse = mock(HttpResponse.class);
        when(hgetResponse.statusCode()).thenReturn(200);
        when(hgetResponse.body()).thenReturn(msgJson);

        // Mock LPUSH to requeue
        HttpResponse<String> lpushResponse = mock(HttpResponse.class);
        when(lpushResponse.statusCode()).thenReturn(200);

        // Mock HDEL from inflight
        HttpResponse<String> hdelResponse = mock(HttpResponse.class);
        when(hdelResponse.statusCode()).thenReturn(200);

        when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(keysResponse)   // HKEYS inflight
                .thenReturn(hgetResponse)   // HGET inflight
                .thenReturn(lpushResponse)  // LPUSH back to queue
                .thenReturn(hdelResponse);  // HDEL from inflight

        // Call the private method via reflection or temporarily make it package-private
        service.requeueExpiredMessages();

        // If no exceptions thrown, the method executed successfully
    }

}
