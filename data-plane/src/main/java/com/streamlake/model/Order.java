package com.streamlake.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Plain-old Java object that maps to a Kafka order event.
 *
 * kafkaOffset / kafkaPartition are NOT part of the JSON payload —
 * they are injected by the TableWorker after poll() so we can audit
 * exactly which Kafka messages are inside each Iceberg snapshot.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Order {

    @JsonProperty("order_id")    private String orderId;
    @JsonProperty("customer_id") private String customerId;
    @JsonProperty("product_id")  private String productId;
    @JsonProperty("quantity")    private int    quantity;
    @JsonProperty("price")       private double price;
    @JsonProperty("status")      private String status;
    @JsonProperty("event_time")  private String eventTime;

    // Set by the consumer worker — never serialised to JSON
    private long kafkaOffset    = -1;
    private int  kafkaPartition = 0;

    // ── getters ───────────────────────────────────────────────────────────────

    public String getOrderId()       { return orderId; }
    public String getCustomerId()    { return customerId; }
    public String getProductId()     { return productId; }
    public int    getQuantity()      { return quantity; }
    public double getPrice()         { return price; }
    public String getStatus()        { return status; }
    public String getEventTime()     { return eventTime; }
    public long   getKafkaOffset()   { return kafkaOffset; }
    public int    getKafkaPartition(){ return kafkaPartition; }

    // ── setters ───────────────────────────────────────────────────────────────

    public void setOrderId(String v)        { orderId = v; }
    public void setCustomerId(String v)     { customerId = v; }
    public void setProductId(String v)      { productId = v; }
    public void setQuantity(int v)          { quantity = v; }
    public void setPrice(double v)          { price = v; }
    public void setStatus(String v)         { status = v; }
    public void setEventTime(String v)      { eventTime = v; }
    public void setKafkaOffset(long v)      { kafkaOffset = v; }
    public void setKafkaPartition(int v)    { kafkaPartition = v; }

    @Override
    public String toString() {
        return String.format("Order{id=%s, product=%s, qty=%d, price=%.2f, status=%s}",
            orderId, productId, quantity, price, status);
    }
}
