package org.example;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.TimeUnit;

public class InMemoryPriorityQueueService implements QueueService {
    private final Map<String, PriorityBlockingQueue<Message>> queues;
    private final long visibilityTimeout;

    // Comparator for priority queue: higher priority first, then FIFO by createdAt
    private final Comparator<Message> messageComparator =
            Comparator.comparingInt(Message::getPriority).reversed()
                    .thenComparingLong(Message::getCreatedAt);

    public InMemoryPriorityQueueService() {
        this.queues = new ConcurrentHashMap<>();
        String propFileName = "config.properties";
        Properties confInfo = new Properties();

        try (InputStream inStream = getClass().getClassLoader().getResourceAsStream(propFileName)) {
            if (inStream != null) {
                confInfo.load(inStream);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.visibilityTimeout = Integer.parseInt(confInfo.getProperty("visibilityTimeout", "30"));
    }

    /**
     * Push message with explicit priority into queue
     */
    public void push(String queueUrl, String msgBody, int priority) {
        queues.computeIfAbsent(queueUrl,
                q -> new PriorityBlockingQueue<>(11, messageComparator));
        queues.get(queueUrl).add(new Message(msgBody, priority));
    }

    @Override
    public void push(String queueUrl, String msgBody) {
        // Default priority = 0 if not specified
        push(queueUrl, msgBody, 0);
    }

    @Override
    public Message pull(String queueUrl) {
        PriorityBlockingQueue<Message> queue = queues.get(queueUrl);
        if (queue == null) {
            return null;
        }

        long nowTime = now();

        Message msg = queue.peek(); // look at head of priority queue
        if (msg == null || !msg.isVisibleAt(nowTime)) {
            return null; // empty or head not visible
        }

        // Mark as delivered
        msg.setReceiptId(UUID.randomUUID().toString());
        msg.incrementAttempts();
        msg.setVisibleFrom(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(visibilityTimeout));

        // Return lightweight copy (don’t expose internal state)
        return new Message(msg.getBody(), msg.getReceiptId());
    }


    @Override
    public void delete(String queueUrl, String receiptId) {
        PriorityBlockingQueue<Message> queue = queues.get(queueUrl);
        if (queue != null) {
            long nowTime = now();

            // Use iterator to find matching receiptId
            for (Iterator<Message> it = queue.iterator(); it.hasNext();) {
                Message msg = it.next();
                if (!msg.isVisibleAt(nowTime) && msg.getReceiptId().equals(receiptId)) {
                    it.remove();
                    break;
                }
            }
        }
    }

    long now() {
        return System.currentTimeMillis();
    }
}
