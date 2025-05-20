-- Initialize the PostgreSQL database for the log processing system

-- Create the logs table to store log metadata
CREATE TABLE IF NOT EXISTS logs (
    id SERIAL PRIMARY KEY,
    log_id VARCHAR(255) NOT NULL UNIQUE,
    source VARCHAR(255),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    content TEXT,
    processed BOOLEAN DEFAULT FALSE
);

-- Create the regex_results table to store pattern matches
CREATE TABLE IF NOT EXISTS regex_results (
    id SERIAL PRIMARY KEY,
    log_id VARCHAR(255) NOT NULL,
    regex_id VARCHAR(50) NOT NULL,
    line_number INTEGER NOT NULL,
    match TEXT NOT NULL,
    line_content TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (log_id) REFERENCES logs(log_id) ON DELETE CASCADE
);

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS idx_regex_results_log_id ON regex_results(log_id);
CREATE INDEX IF NOT EXISTS idx_regex_results_regex_id ON regex_results(regex_id);

-- Create a view for easy querying of common patterns
CREATE OR REPLACE VIEW error_logs AS
SELECT l.log_id, l.source, l.timestamp, r.line_number, r.match, r.line_content
FROM logs l
JOIN regex_results r ON l.log_id = r.log_id
WHERE r.regex_id = 'error'
ORDER BY l.timestamp DESC, r.line_number;

-- Function to get regex matches for a specific log
CREATE OR REPLACE FUNCTION get_log_matches(log_id_param VARCHAR)
RETURNS TABLE (
    regex_id VARCHAR,
    line_number INTEGER,
    match TEXT,
    line_content TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT r.regex_id, r.line_number, r.match, r.line_content
    FROM regex_results r
    WHERE r.log_id = log_id_param
    ORDER BY r.line_number;
END;
$$ LANGUAGE plpgsql;