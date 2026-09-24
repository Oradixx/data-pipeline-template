-- Raw readings, one row per (time, device, metric).
-- "Long" format: adding a new metric needs no schema change.
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- tsdb.hypertable turns the table into a hypertable, automatically partitioned
-- ("chunked") on its first timestamp column. Old chunks are converted to the
-- columnstore (compressed, fast for analytics); segmentby/orderby tell TimescaleDB
-- how to group and sort rows inside the compressed chunks.
CREATE TABLE sensor_readings (
    time       TIMESTAMPTZ      NOT NULL,
    device_id  TEXT             NOT NULL,
    metric     TEXT             NOT NULL,
    value      DOUBLE PRECISION NOT NULL
) WITH (
    tsdb.hypertable,
    tsdb.segmentby = 'device_id, metric',
    tsdb.orderby   = 'time DESC'
);

-- Typical query: "metric X of device Y over the last N hours".
CREATE INDEX sensor_readings_device_metric_time_idx
    ON sensor_readings (device_id, metric, time DESC);
