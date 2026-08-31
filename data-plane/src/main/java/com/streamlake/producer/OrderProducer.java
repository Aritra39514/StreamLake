package com.streamlake.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamlake.model.Order;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Idempotent Kafka producer that generates fake e-commerce order events.
 *
 * Key Confluent patterns demonstrated:
 *  - enable.idempotence=true       → exactly-once to the broker (PID + sequence no.)
 *  - acks=all                      → waits for all in-sync replicas
 *  - compression.type=snappy       → ~3x smaller on the wire
 *  - linger.ms + batch.size        → micro-batching for throughput
 *  - customer_id as message key    → guarantees per-customer ordering
 */
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String[] PRODUCTS = {"prod-001","prod-002","prod-003","prod-004","prod-005"};
    private static final double[] PRICES   = {9.99, 24.99, 49.99, 99.99, 199.99};
    private static final String[] STATUSES = {"PLACED","CONFIRMED","SHIPPED","DELIVERED","CANCELLED"};

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final String bootstrapServers;

    public OrderProducer(String bootstrapServers, String topic) {
        this.topic = topic;
        this.bootstrapServers = bootstrapServers;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,  bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,               "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,   "snappy");
        props.put(ProducerConfig.LINGER_MS_CONFIG,          5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,         65536);
        props.put(ProducerConfig.RETRIES_CONFIG,            Integer.MAX_VALUE);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        this.producer = new KafkaProducer<>(props);
    }

    public void ensureTopic(int partitions) {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient admin = AdminClient.create(adminProps)) {
            Set<String> existing = admin.listTopics().names().get();
            if (!existing.contains(topic)) {
                admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
                log.info("Created topic '{}' ({} partitions)", topic, partitions);
            } else {
                log.info("Topic '{}' already exists", topic);
            }
        } catch (Exception e) {
            log.warn("Could not ensure topic: {}", e.getMessage());
        }
    }

    public Order generateOrder() {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        int idx = rng.nextInt(PRODUCTS.length);
        int qty = rng.nextInt(1, 6);

        Order o = new Order();
        o.setOrderId(UUID.randomUUID().toString());
        o.setCustomerId("cust-" + rng.nextInt(1000, 9999));
        o.setProductId(PRODUCTS[idx]);
        o.setQuantity(qty);
        o.setPrice(Math.round(PRICES[idx] * qty * 100.0) / 100.0);
        o.setStatus(STATUSES[rng.nextInt(STATUSES.length)]);
        o.setEventTime(Instant.now().toString());
        return o;
    }

    public void produce(double ratePerSec, long totalCount) throws Exception {
        ensureTopic(3);
        long intervalMs = Math.max(1, (long) (1000.0 / ratePerSec));
        long sent = 0;

        log.info("Producing to '{}' at {}/sec  (Ctrl-C to stop)", topic, ratePerSec);
        try {
            while (totalCount == 0 || sent < totalCount) {
                Order order = generateOrder();
                String json  = MAPPER.writeValueAsString(order);

                producer.send(
                    new ProducerRecord<>(topic, order.getCustomerId(), json),
                    (meta, err) -> {
                        if (err != null) log.error("Delivery failed: {}", err.getMessage());
                    }
                );
                if (++sent % 100 == 0) log.info("Produced {} events", sent);
                Thread.sleep(intervalMs);
            }
        } finally {
            producer.flush();
            producer.close();
            log.info("Producer stopped. Total sent: {}", sent);
        }
    }
}
