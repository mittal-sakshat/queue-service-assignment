package org.example;

public class Message {
    /** How many times this message has been delivered. */
    private int attempts;

    /** Visible from time */
    private long visibleFrom;

    /** An identifier associated with the act of receiving the message. */
    private String receiptId;

    private String msgBody;

    /** Message priority (higher number = higher priority) */
    private int priority;

    /** Timestamp when message was created (used for FIFO within same priority) */
    private long createdAt;

    public Message(String msgBody, int priority) {
        this.msgBody = msgBody;
        this.priority = priority;
        this.createdAt = System.currentTimeMillis();
    }

    public Message(String msgBody, String receiptId) {
        this.msgBody = msgBody;
        this.receiptId = receiptId;
        this.createdAt = System.currentTimeMillis();
    }

    public String getReceiptId() {
        return this.receiptId;
    }

    protected void setReceiptId(String receiptId) {
        this.receiptId = receiptId;
    }

    protected void setVisibleFrom(long visibleFrom) {
        this.visibleFrom = visibleFrom;
    }

    public boolean isVisibleAt(long instant) {
        return visibleFrom < instant;
    }

    public String getBody() {
        return msgBody;
    }

    protected int getAttempts() {
        return attempts;
    }

    protected void incrementAttempts() {
        this.attempts++;
    }

    public int getPriority() {
        return priority;
    }

    public long getCreatedAt() {
        return createdAt;
    }
}
