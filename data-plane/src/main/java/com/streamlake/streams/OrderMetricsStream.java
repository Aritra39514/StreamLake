package com.streamlake.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamlake.model.Order;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

/**
 * Kafka Streams application — two real-world streaming pipelines over the
 * 'orders' topic:
 *
 *  Pipeline A — Filter & Forward
 *    orders (all) → filter PLACED → orders-placed
 *    Use case: downstream systems only care about new orders, not updates.
 *
 *  Pipeline B — Revenue aggregation (tumbling window)
 *    orders → rekey by product_id → 1-minute tumbling window → SUM(price)
 *    → product-revenue-metrics
 *    Use case: real-time dashboards, anomaly detection on revenue spikes.
 *
 * Key Kafka Streams concepts shown:
 *  - KStream.filter()         → stateless predicate
 *  - KStream.selectKey()      → repartitioning (causes a network shuffle)
 *  - KGroupedStream.windowedBy() + aggregate() → stateful windowed aggregation
 *  - TimeWindows.ofSizeWithNoGrace() → tumbling window, no late-arrival grace
 *  - KTable → materialised view (queryable via Interactive Queries)
 */
public class OrderMetricsStream {

    private static final Logger log = LoggerFactory.getLogger(OrderMetricsStream.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final KafkaStreams streams;

    public OrderMetricsStream(String bootstrapServers) {
        StreamsBuilder builder = new StreamsBuilder();

        // ── Source: raw JSON orders ────────────────────────────────────────────
        KStream<String, String> rawOrders = builder.stream(
            "orders",
            Consumed.with(Serdes.String(), Serdes.String())
        );

        // Parse JSON → Order (null on parse error → filtered out below)
        KStream<String, Order> orders = rawOrders
            .mapValues(json -> {
                try { return MAPPER.readValue(json, Order.class); }
                catch (Exception e) { return null; }
            })
            .filter((k, v) -> v != null);

        // ── Pipeline A: filter PLACED → forward to orders-placed ──────────────
        orders
            .filter((customerId, order) -> "PLACED".equals(order.getStatus()))
            .mapValues(order -> {
                try { return MAPPER.writeValueAsString(order); }
                catch (Exception e) { return null; }
            })
            .filter((k, v) -> v != null)
            .to("orders-placed", Produced.with(Serdes.String(), Serdes.String()));

        // ── Pipeline B: revenue per product per 1-minute window ───────────────
        //
        // selectKey() causes a repartition — Kafka Streams writes an internal
        // topic and re-reads it to guarantee all records for the same key land
        // on the same partition before the aggregation.
        orders
            .filter((k, order) -> order.getPrice() > 0)
            .selectKey((customerId, order) -> order.getProductId())
            .groupByKey(Grouped.with(Serdes.String(), new OrderSerde()))
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
            .aggregate(
                () -> 0.0,                                   // initialiser
                (productId, order, total) -> total + order.getPrice(), // aggregator
                Materialized.with(Serdes.String(), Serdes.Double())
            )
            .toStream()
            .map((windowedKey, revenue) -> KeyValue.pair(
                windowedKey.key(),
                String.format(
                    "{\"product\":\"%s\",\"window_start_ms\":%d,\"revenue_usd\":%.2f}",
                    windowedKey.key(),
                    windowedKey.window().startTime().toEpochMilli(),
                    revenue
                )
            ))
            .to("product-revenue-metrics", Produced.with(Serdes.String(), Serdes.String()));

        // ── Streams config ─────────────────────────────────────────────────────
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,       "streamlake-order-metrics");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,    bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,   Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,   1000);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,   2);

        this.streams = new KafkaStreams(builder.build(), props);
        this.streams.setUncaughtExceptionHandler((t, e) ->
            log.error("Uncaught streams exception on thread {}", t.getName(), e));
    }

    public void start() {
        streams.start();
        log.info("Kafka Streams app started  (topology: orders → filter+aggregate → output topics)");
    }

    public void stop() {
        streams.close(Duration.ofSeconds(10));
        log.info("Kafka Streams app stopped");
    }
}
