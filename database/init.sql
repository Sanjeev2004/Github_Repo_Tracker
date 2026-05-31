CREATE TABLE IF NOT EXISTS repository_activity (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    repository_name TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_count INTEGER NOT NULL,
    PRIMARY KEY (window_start, window_end, repository_name, event_type)
);

CREATE INDEX IF NOT EXISTS idx_repo_activity_window ON repository_activity (window_start DESC);
CREATE INDEX IF NOT EXISTS idx_repo_activity_repo ON repository_activity (repository_name);
