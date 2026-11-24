# Data platforms

Network telemetry is most valuable next to the rest of your data: capacity
planning in a warehouse, ML on a lakehouse, joins with inventory and
tickets. The pipeline reaches data platforms in three ways:

| Path | Latency | Reaches |
|---|---|---|
| [`parquet`](../configuration/outputs.md#parquet-data-lake-object-storage) files on object storage | minutes | Snowflake, Databricks, BigQuery, Athena, Trino, Spark, ClickHouse, DuckDB: anything that reads Parquet |
| [`clickhouse`](../configuration/outputs.md#clickhouse) native | seconds | ClickHouse (self-hosted or Cloud) |
| [`otlp`](../configuration/outputs.md#otlp-opentelemetry) / [`kafka`](../configuration/outputs.md#kafka-apache-kafka-redpanda) | seconds | OpenTelemetry Collector exporters; each platform's Kafka connector |

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

## Choosing between paths

- **Minutes of latency are fine; many consumers:** Parquet on object storage
  is the cheapest and most portable option. Every platform reads it.
- **Real-time operational dashboards on SQL:** the native ClickHouse output.
- **You already run a Kafka platform:** the Kafka output plus your
  platform's connector.
