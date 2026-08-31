package com.streamlake.iceberg;

import com.streamlake.model.Order;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * Writes batches of Order objects into an Apache Iceberg table stored on the
 * local filesystem (HadoopCatalog + Parquet format).
 *
 * How an Iceberg write works (the Confluent Tableflow core):
 *
 *   1. Create a Parquet file in the table's data/ directory.
 *   2. Build a DataFile descriptor with column-level statistics (Metrics).
 *      These stats are what make Iceberg fast — query engines skip whole
 *      files based on min/max without reading them.
 *   3. Open a new AppendFiles transaction, attach the DataFile, and commit().
 *      Commit is atomic: it writes a new JSON snapshot file and updates
 *      the metadata.json pointer — no data is ever modified in place.
 *   4. The table's current snapshot now points to the new data file.
 *      Old snapshots remain intact (time-travel / rollback work for free).
 *
 * Upgrading to a production catalog (AWS Glue, Unity Catalog, Nessie):
 *   Swap HadoopCatalog → GlueCatalog / RESTCatalog — the write path is
 *   identical because Iceberg's catalog API is fully abstracted.
 */
public class IcebergTableWriter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(IcebergTableWriter.class);

    private static final Schema ORDERS_SCHEMA = new Schema(
        Types.NestedField.required(1,  "order_id",        Types.StringType.get()),
        Types.NestedField.optional(2,  "customer_id",     Types.StringType.get()),
        Types.NestedField.optional(3,  "product_id",      Types.StringType.get()),
        Types.NestedField.optional(4,  "quantity",        Types.IntegerType.get()),
        Types.NestedField.optional(5,  "price",           Types.DoubleType.get()),
        Types.NestedField.optional(6,  "status",          Types.StringType.get()),
        Types.NestedField.optional(7,  "event_time",      Types.StringType.get()),
        Types.NestedField.optional(8,  "kafka_offset",    Types.LongType.get()),
        Types.NestedField.optional(9,  "kafka_partition", Types.IntegerType.get())
    );

    private final Table table;
    private final HadoopCatalog catalog;

    public IcebergTableWriter(String warehousePath, String tableName) {
        Configuration conf = new Configuration();
        catalog = new HadoopCatalog(conf, warehousePath);

        Namespace ns = Namespace.of("streamlake");
        try {
            catalog.createNamespace(ns);
        } catch (AlreadyExistsException ignored) {}

        TableIdentifier id = TableIdentifier.of(ns, tableName);
        if (!catalog.tableExists(id)) {
            this.table = catalog.createTable(id, ORDERS_SCHEMA);
            log.info("Created Iceberg table {}", id);
        } else {
            this.table = catalog.loadTable(id);
            log.info("Loaded Iceberg table {}  (snapshot seq={})",
                id, table.currentSnapshot() == null ? 0 : table.currentSnapshot().sequenceNumber());
        }
    }

    /**
     * Append a batch of orders to the Iceberg table as one atomic snapshot.
     * Returns the number of rows written.
     */
    public int append(List<Order> orders) throws IOException {
        if (orders.isEmpty()) return 0;

        // Step 1 — write Parquet file to the table's data directory
        String location = table.location() + "/data/" + UUID.randomUUID() + ".parquet";
        OutputFile outputFile = table.io().newOutputFile(location);

        FileAppender<GenericRecord> appender = Parquet.write(outputFile)
            .schema(table.schema())
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .overwrite()
            .build();

        try (appender) {
            for (Order o : orders) {
                GenericRecord record = GenericRecord.create(table.schema());
                record.setField("order_id",        o.getOrderId());
                record.setField("customer_id",     o.getCustomerId());
                record.setField("product_id",      o.getProductId());
                record.setField("quantity",        o.getQuantity());
                record.setField("price",           o.getPrice());
                record.setField("status",          o.getStatus());
                record.setField("event_time",      o.getEventTime());
                record.setField("kafka_offset",    o.getKafkaOffset());
                record.setField("kafka_partition", o.getKafkaPartition());
                appender.add(record);
            }
        }

        // Step 2 — build a DataFile descriptor with column-level statistics
        DataFile dataFile = DataFiles.builder(table.spec())
            .withInputFile(table.io().newInputFile(outputFile.location()))
            .withMetrics(appender.metrics())
            .build();

        // Step 3 — atomic snapshot commit (no existing data is touched)
        table.newAppend()
            .appendFile(dataFile)
            .commit();

        log.debug("Iceberg commit: {} rows → {} (seq {})",
            orders.size(), outputFile.location(),
            table.currentSnapshot().sequenceNumber());

        return orders.size();
    }

    /** Returns the total row count from the latest snapshot summary. */
    public long rowCount() {
        Snapshot snap = table.currentSnapshot();
        if (snap == null) return 0;
        String v = snap.summary().get("total-records");
        return v != null ? Long.parseLong(v) : 0;
    }

    /** Refreshes metadata — needed after another process commits a snapshot. */
    public void refresh() {
        table.refresh();
    }

    @Override
    public void close() throws IOException {
        catalog.close();
    }
}
