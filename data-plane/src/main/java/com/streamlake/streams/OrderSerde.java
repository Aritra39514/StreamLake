package com.streamlake.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamlake.model.Order;
import org.apache.kafka.common.serialization.*;

import java.util.Map;

/**
 * Custom Kafka Serde for Order objects.
 * Plugs into the Kafka Streams DSL so the topology is type-safe.
 */
public class OrderSerde implements Serde<Order> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public void close() {}

    @Override
    public Serializer<Order> serializer() {
        return (topic, order) -> {
            if (order == null) return null;
            try {
                return MAPPER.writeValueAsBytes(order);
            } catch (Exception e) {
                throw new RuntimeException("Order serialization failed", e);
            }
        };
    }

    @Override
    public Deserializer<Order> deserializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.readValue(data, Order.class);
            } catch (Exception e) {
                throw new RuntimeException("Order deserialization failed", e);
            }
        };
    }
}
