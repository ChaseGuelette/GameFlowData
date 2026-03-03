-- 007_job_executions.sql
-- Persistent job execution history for pipeline observability and debugging.
-- Tracks every scheduler job run: status, duration, errors, and metrics.

CREATE TABLE IF NOT EXISTS job_executions (
    id SERIAL PRIMARY KEY,
    job_name TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    status TEXT NOT NULL,  -- 'running', 'success', 'failed', 'timeout'
    duration_seconds FLOAT,
    error_message TEXT,
    metrics JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_job_exec_name_started
    ON job_executions(job_name, started_at DESC);
