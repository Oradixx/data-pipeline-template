-- migrate:no-transaction
-- (continuous aggregates cannot be created inside a transaction)

-- A continuous aggregate is a materialized view that TimescaleDB refreshes
-- incrementally: dashboards read pre-computed hourly stats instead of scanning
-- every raw row. materialized_only = false also includes the most recent,
-- not-yet-materialized data ("real-time aggregation").
CREATE MATERIALIZED VIEW readings_hourly
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    time_bucket(INTERVAL '1 hour', time) AS bucket,
    device_id,
    metric,
    avg(value)  AS avg_value,
    min(value)  AS min_value,
    max(value)  AS max_value,
    count(*)    AS n_readings
FROM sensor_readings
GROUP BY bucket, device_id, metric
WITH NO DATA;

-- Refresh the last 3 days every 15 minutes (the current hour is left to real-time aggregation).
SELECT add_continuous_aggregate_policy(
    'readings_hourly',
    start_offset      => INTERVAL '3 days',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '15 minutes'
);
