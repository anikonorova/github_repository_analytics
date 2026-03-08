CREATE SCHEMA IF NOT EXISTS raw_data;

CREATE TABLE IF NOT EXISTS raw_data.pull_requests (
    id            VARCHAR,
    payload       JSON,
    _extracted_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_data.reviews (
    id            VARCHAR,
    pr_id         VARCHAR,
    payload       JSON,
    _extracted_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_data.commits (
    sha           VARCHAR,
    payload       JSON,
    _extracted_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_data.issues (
    id            VARCHAR,
    payload       JSON,
    _extracted_at TIMESTAMP
);