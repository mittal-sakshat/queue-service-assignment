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
}
