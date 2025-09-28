package org.example;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryPriorityQueueServiceTest {

    private InMemoryPriorityQueueService service;

    @BeforeEach
    void setUp() {
        service = new InMemoryPriorityQueueService();
    }

    @Test
    void testPushAndPullSingleMessage() {
        service.push("q1", "hello", 1);

        Message msg = service.pull("q1");
        assertNotNull(msg);
        assertEquals("hello", msg.getBody());
        assertNotNull(msg.getReceiptId());
    }

    @Test
    void testFifoOrderWithinSamePriority() {
        service.push("q1", "msg1", 1);
        service.push("q1", "msg2", 1);

        Message m1 = service.pull("q1");
        Message m2 = service.pull("q1");

        assertEquals("msg1", m1.getBody());
        assertEquals("msg2", m2.getBody());
    }

    @Test
    void testPriorityOrder() {
        service.push("q1", "low", 1);
        service.push("q1", "high", 5);

        Message first = service.pull("q1");
        assertEquals("high", first.getBody());
    }

    @Test
    void testVisibilityTimeout() throws InterruptedException {
        service.push("q1", "retry", 1);

        Message pulled = service.pull("q1");
        assertNotNull(pulled);

        // Immediately pulling again should return null (message invisible)
        Message secondPull = service.pull("q1");
        assertNull(secondPull);

        // Wait longer than visibility timeout (30s default in config.properties)
        Thread.sleep(31000);

        Message retried = service.pull("q1");
        assertNotNull(retried);
        assertEquals("retry", retried.getBody());
    }

    @Test
    void testDeleteRemovesMessage() {
        service.push("q1", "toDelete", 1);

        Message pulled = service.pull("q1");
        assertNotNull(pulled);

        // Delete it using receiptId
        service.delete("q1", pulled.getReceiptId());

        // After deletion, it should not reappear
        Message again = service.pull("q1");
        assertNull(again);
    }
}
