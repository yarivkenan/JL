-- +goose Up
-- Alert evaluations are now stored as timeseries data points in the
-- data_points table (metric name "alert.status", value 1=firing / 0=resolved).
-- No additional schema is required.

-- +goose Down
