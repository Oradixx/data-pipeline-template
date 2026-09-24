-- Keep raw readings for 90 days; the hourly aggregate keeps the long-term history.
SELECT add_retention_policy('sensor_readings', drop_after => INTERVAL '90 days');
