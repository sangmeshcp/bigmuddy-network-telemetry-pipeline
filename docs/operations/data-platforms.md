# Data platforms

Network telemetry is most valuable next to the rest of your data: capacity
planning in a warehouse, ML on a lakehouse, joins with inventory and
tickets. The pipeline reaches data platforms in three ways:

| Path | Latency | Reaches |
|---|---|---|
| [`iceberg`](../configuration/outputs.md#iceberg-apache-iceberg-tables) tables via a catalog | minutes | Snowflake, Databricks, BigQuery, Athena, Trino, Spark, Flink, Dremio, DuckDB, ClickHouse, **as native tables** |
| [`parquet`](../configuration/outputs.md#parquet-data-lake-object-storage) files on object storage | minutes | Snowflake, Databricks, BigQuery, Athena, Trino, Spark, ClickHouse, DuckDB: anything that reads Parquet |
| [`clickhouse`](../configuration/outputs.md#clickhouse) native | seconds | ClickHouse (self-hosted or Cloud) |
| [`kafka`](../configuration/outputs.md#avro-and-schema-registry) with `format: avro` | seconds | Each platform's own Kafka connector: Snowflake, Databricks, BigQuery, ClickHouse Kafka engine, S3 / Iceberg sink connectors |
| [`otlp`](../configuration/outputs.md#otlp-opentelemetry) | seconds | OpenTelemetry Collector exporters |

All analytical sinks share one [table layout](../concepts/data-model.md#analytical-row-layout),
so queries work the same everywhere.

## Recommended pipeline config

```yaml
processors:
  - {type: metric_types, presets: [network]}   # mark counters
  - {type: downsample, interval: 1m}           # optional: cut volume for the lake
outputs:
  - name: lake
    type: parquet
    url: s3://my-bucket/network
    batch_size: 200000
    flush_interval: 5m
    queue: {type: disk, dir: /var/lib/pipeline/queue/lake, max_bytes: 21474836480}
```

## Iceberg tables

If you run an Iceberg catalog, prefer the `iceberg` output: every engine sees
a proper table (schema, partitions, snapshots, time travel) and nothing needs
loading.

| Engine | How it reads the table |
|---|---|
| Snowflake | Catalog integration to your REST catalog (Polaris / Snowflake Open Catalog), then `CREATE ICEBERG TABLE … CATALOG = '<integration>' CATALOG_TABLE_NAME = 'telemetry'` |
| Databricks | Tables in Unity Catalog are readable directly; external catalogs via Lakehouse Federation |
| BigQuery | BigLake Iceberg tables / the BigLake metastore |
| Athena, Trino, Starburst, Spark, Flink, Dremio | Iceberg connector pointed at the same catalog |
| DuckDB | `INSTALL iceberg;` then `iceberg_scan(...)` or attach the REST catalog |
| ClickHouse | `icebergS3(...)` table function or an Iceberg database engine |

!!! info "What has been verified"
    CI writes through an Apache Iceberg REST catalog with data on MinIO,
    retries the batch (one snapshot results), and scans the table back.
    A table written by the pipeline is also read by **PyIceberg**, an
    independent Iceberg implementation, with schema, partition spec, snapshot
    properties, all values and a filtered scan checked.

## Loading the Parquet lake

The examples assume files under `s3://my-bucket/network/telemetry/`.

!!! info "What has been verified"
    The file format, schema and Hive partitioning are checked in CI with
    DuckDB and ClickHouse reading the pipeline's output. The Snowflake,
    Databricks, BigQuery and Athena statements follow each vendor's
    documented syntax for Parquet; adapt names, credentials and integrations
    to your account.

=== "Snowflake"

    ```sql
    -- One-time: a storage integration for the bucket (see Snowflake docs), then:
    CREATE STAGE net_lake
      URL = 's3://my-bucket/network/telemetry/'
      STORAGE_INTEGRATION = my_s3_integration
      FILE_FORMAT = (TYPE = PARQUET);

    CREATE TABLE telemetry (
      timestamp TIMESTAMP_TZ, source STRING, name STRING, field STRING,
      tags VARIANT, metric_type STRING, value_type STRING,
      value_double DOUBLE, value_int NUMBER(19,0), value_string STRING);

    -- Continuous loading with Snowpipe (needs S3 event notifications):
    CREATE PIPE telemetry_pipe AUTO_INGEST = TRUE AS
      COPY INTO telemetry FROM @net_lake
      FILE_FORMAT = (TYPE = PARQUET)
      MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

    SELECT tags:interface_name::STRING AS interface, max(value_double)
    FROM telemetry
    WHERE field = 'state/counters/in-octets'
    GROUP BY 1;
    ```

=== "Databricks"

    ```python
    # Auto Loader: incrementally ingests new files into a Delta table.
    (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/Volumes/net/raw/_schemas/telemetry")
        .load("s3://my-bucket/network/telemetry/")
        .writeStream
        .option("checkpointLocation", "/Volumes/net/raw/_checkpoints/telemetry")
        .trigger(availableNow=True)          # or processingTime="1 minute"
        .toTable("net.telemetry"))
    ```

    Or in SQL: `COPY INTO net.telemetry FROM 's3://my-bucket/network/telemetry/' FILEFORMAT = PARQUET;`

=== "BigQuery"

    Write to GCS with the `parquet` output (`s3.endpoint:
    storage.googleapis.com` and HMAC keys), then:

    ```sql
    CREATE EXTERNAL TABLE net.telemetry_ext
    WITH PARTITION COLUMNS
    OPTIONS (
      format = 'PARQUET',
      uris = ['gs://my-bucket/network/telemetry/*'],
      hive_partition_uri_prefix = 'gs://my-bucket/network/telemetry');
    ```

    Use `LOAD DATA INTO` (or a scheduled query) to materialise a native
    table. The `tags` map arrives as a repeated key/value record.

=== "Athena / Trino"

    ```sql
    CREATE EXTERNAL TABLE telemetry (
      `timestamp` timestamp, source string, name string, field string,
      tags map<string,string>, metric_type string, value_type string,
      value_double double, value_int bigint, value_string string)
    PARTITIONED BY (dt date, hour int)
    STORED AS PARQUET
    LOCATION 's3://my-bucket/network/telemetry/'
    TBLPROPERTIES (
      'projection.enabled' = 'true',
      'projection.dt.type' = 'date', 'projection.dt.format' = 'yyyy-MM-dd',
      'projection.dt.range' = '2026-01-01,NOW',
      'projection.hour.type' = 'integer', 'projection.hour.range' = '0,23', 'projection.hour.digits' = '2');
    ```

=== "ClickHouse"

    ```sql
    SELECT tags['interface_name'] AS interface, max(value_double)
    FROM s3('https://my-bucket.s3.amazonaws.com/network/telemetry/*/*/*.parquet', 'Parquet')
    WHERE field = 'state/counters/in-octets'
    GROUP BY interface;
    ```

    For real-time dashboards use the native [`clickhouse`](../configuration/outputs.md#clickhouse) output instead.

=== "DuckDB"

    ```sql
    INSTALL httpfs; LOAD httpfs;
    SELECT dt, source, count(*)
    FROM read_parquet('s3://my-bucket/network/telemetry/*/*/*.parquet', hive_partitioning = true)
    WHERE dt = DATE '2026-11-24'
    GROUP BY ALL;
    ```

## Through Kafka (Avro + Schema Registry)

Use [`format: avro`](../configuration/outputs.md#avro-and-schema-registry)
when the platform team already runs Kafka and Kafka Connect. The records
carry their schema ID, so connectors type every column without
configuration.

=== "ClickHouse Kafka engine"

    ```sql
    CREATE TABLE telemetry_queue (
        timestamp DateTime64(6, 'UTC'), source String, name String, field String,
        tags Map(String, String), metric_type String, value_type String,
        value_double Nullable(Float64), value_int Nullable(Int64), value_string Nullable(String)
    ) ENGINE = Kafka SETTINGS
        kafka_broker_list = 'kafka-1:9092',
        kafka_topic_list = 'network.telemetry.rows',
        kafka_group_name = 'clickhouse',
        kafka_format = 'AvroConfluent',
        format_avro_schema_registry_url = 'http://schema-registry:8081';

    CREATE MATERIALIZED VIEW telemetry_mv TO telemetry AS SELECT * FROM telemetry_queue;
    ```

    `telemetry` is a MergeTree table such as the one the `clickhouse` output
    creates. CI decodes this output's records with ClickHouse's
    `AvroConfluent` reader against Redpanda's registry.

=== "Snowflake"

    Snowflake Kafka connector (Kafka Connect) with Snowpipe Streaming:

    ```properties
    connector.class=com.snowflake.kafka.connector.SnowflakeSinkConnector
    topics=network.telemetry.rows
    snowflake.ingestion.method=SNOWPIPE_STREAMING
    snowflake.enable.schematization=true
    value.converter=io.confluent.connect.avro.AvroConverter
    value.converter.schema.registry.url=https://schema-registry:8081
    key.converter=org.apache.kafka.connect.storage.StringConverter
    ```

    With schematization, each Avro field becomes a column.

=== "Databricks"

    ```python
    from pyspark.sql.avro.functions import from_avro
    rows = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka-1:9092")
        .option("subscribe", "network.telemetry.rows").load()
        .select(from_avro("value", options={"mode": "PERMISSIVE"},
                          subject="network.telemetry.rows-value",
                          schemaRegistryAddress="https://schema-registry:8081").alias("r"))
        .select("r.*"))
    rows.writeStream.toTable("network.telemetry")
    ```

=== "BigQuery / S3 / Iceberg"

    Any Kafka Connect sink with `value.converter=io.confluent.connect.avro.AvroConverter`
    works: the BigQuery sink connector, the Confluent S3 sink
    (`format.class=io.confluent.connect.s3.format.parquet.ParquetFormat`), or
    the Apache Iceberg sink connector.

## Choosing between paths

- **Minutes of latency are fine; many consumers:** Parquet on object storage
  is the cheapest and most portable option. Every platform reads it.
- **Real-time operational dashboards on SQL:** the native ClickHouse output.
- **You already run a Kafka platform:** the Kafka output with
  `format: avro`, plus your platform's connector.
