-- 1. Create the database and schema first
CREATE DATABASE IF NOT EXISTS PORTFOLIO_DEMO_DB;
CREATE SCHEMA IF NOT EXISTS PORTFOLIO_DEMO_DB.ECOMMERCE_OPS;

-- 2. Set your active context
USE DATABASE PORTFOLIO_DEMO_DB;
USE SCHEMA ECOMMERCE_OPS;

-- 3. Create the raw landing table for semi-structured JSON data
CREATE OR REPLACE TABLE RAW_WEBHOOK_EVENTS (
    event_id VARCHAR,
    event_timestamp TIMESTAMP_NTZ,
    payload VARIANT
);
-- Insert simulated incoming JSON webhook payloads
INSERT INTO RAW_WEBHOOK_EVENTS (event_id, event_timestamp, payload)
SELECT 'EVT-1001', CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT('user_id', 4512, 'action', 'checkout', 'amount', 1250.00, 'device', 'mobile', 'location', 'Milan')
UNION ALL
SELECT 'EVT-1002', CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT('user_id', 8821, 'action', 'login_failed', 'amount', 0.00, 'device', 'desktop', 'location', 'Unknown')
UNION ALL
SELECT 'EVT-1003', CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT('user_id', 4512, 'action', 'checkout', 'amount', 4300.00, 'device', 'mobile', 'location', 'Milan');
-- Query the raw table and extract nested JSON keys using colon notation
SELECT 
    event_id,
    event_timestamp,
    payload:user_id::INT AS user_id,
    payload:action::STRING AS action_type,
    payload:amount::FLOAT AS transaction_amount,
    payload:location::STRING AS location
FROM RAW_WEBHOOK_EVENTS;
-- 5. Create a Snowflake Stream to track live changes (CDC)
CREATE OR REPLACE STREAM WEBHOOK_STREAM ON TABLE RAW_WEBHOOK_EVENTS;
-- 6. Create curated downstream tables for clean transactions and fraud alerts
CREATE OR REPLACE TABLE CLEANED_TRANSACTIONS (
    event_id VARCHAR,
    event_timestamp TIMESTAMP_NTZ,
    user_id INT,
    action_type STRING,
    transaction_amount FLOAT,
    location STRING,
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE FRAUD_ALERTS (
    event_id VARCHAR,
    event_timestamp TIMESTAMP_NTZ,
    user_id INT,
    transaction_amount FLOAT,
    location STRING,
    risk_reason STRING,
    flagged_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
-- 7. Process incoming stream records: route normal transactions vs. high-risk fraud alerts
-- Insert clean transactions
INSERT INTO CLEANED_TRANSACTIONS (event_id, event_timestamp, user_id, action_type, transaction_amount, location)
SELECT 
    event_id,
    event_timestamp,
    payload:user_id::INT,
    payload:action::STRING,
    payload:amount::FLOAT,
    payload:location::STRING
FROM WEBHOOK_STREAM
WHERE METADATA$ACTION = 'INSERT'
  AND payload:amount::FLOAT <= 3000.00;

-- Insert fraud alerts for high-value or suspicious events
INSERT INTO FRAUD_ALERTS (event_id, event_timestamp, user_id, transaction_amount, location, risk_reason)
SELECT 
    event_id,
    event_timestamp,
    payload:user_id::INT,
    payload:amount::FLOAT,
    payload:location::STRING,
    CASE 
        WHEN payload:amount::FLOAT > 3000.00 THEN 'High Value Transaction Threshold Exceeded'
        WHEN payload:location::STRING = 'Unknown' THEN 'Unrecognized Geolocation'
        ELSE 'Suspicious Activity'
    END AS risk_reason
FROM WEBHOOK_STREAM
WHERE METADATA$ACTION = 'INSERT'
  AND (payload:amount::FLOAT > 3000.00 OR payload:location::STRING = 'Unknown');
  -- 8. Create a Snowflake Task to automate the fraud and inventory pipeline
CREATE OR REPLACE TASK PROCESS_WEBHOOK_AUDIT_TASK
  WAREHOUSE = PORTFOLIO_WH
  SCHEDULE = '1 MINUTE'
AS
-- Insert clean transactions
INSERT INTO CLEANED_TRANSACTIONS (event_id, event_timestamp, user_id, action_type, transaction_amount, location)
SELECT 
    event_id,
    event_timestamp,
    payload:user_id::INT,
    payload:action::STRING,
    payload:amount::FLOAT,
    payload:location::STRING
FROM WEBHOOK_STREAM
WHERE METADATA$ACTION = 'INSERT'
  AND payload:amount::FLOAT <= 3000.00;

-- Insert fraud alerts for high-value or suspicious events
INSERT INTO FRAUD_ALERTS (event_id, event_timestamp, user_id, transaction_amount, location, risk_reason)
SELECT 
    event_id,
    event_timestamp,
    payload:user_id::INT,
    payload:amount::FLOAT,
    payload:location::STRING,
    CASE 
        WHEN payload:amount::FLOAT > 3000.00 THEN 'High Value Transaction Threshold Exceeded'
        WHEN payload:location::STRING = 'Unknown' THEN 'Unrecognized Geolocation'
        ELSE 'Suspicious Activity'
    END AS risk_reason
FROM WEBHOOK_STREAM
WHERE METADATA$ACTION = 'INSERT'
  AND (payload:amount::FLOAT > 3000.00 OR payload:location::STRING = 'Unknown');
  -- View clean, normal transactions
SELECT * FROM CLEANED_TRANSACTIONS;

-- View flagged high-value or suspicious fraud alerts
SELECT * FROM FRAUD_ALERTS;

-- 1. Insert fresh test webhooks into the raw table
INSERT INTO RAW_WEBHOOK_EVENTS (event_id, event_timestamp, payload)
SELECT 'EVT-3001', CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT('user_id', 7711, 'action', 'checkout', 'amount', 900.00, 'device', 'mobile', 'location', 'Florence')
UNION ALL
SELECT 'EVT-3002', CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT('user_id', 7722, 'action', 'checkout', 'amount', 6000.00, 'device', 'mobile', 'location', 'Milan')
UNION ALL
SELECT 'EVT-3003', CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT('user_id', 7733, 'action', 'login_failed', 'amount', 0.00, 'device', 'desktop', 'location', 'Unknown');

-- 2. Capture the current stream delta into a temporary staging table (safely consuming the stream once)
CREATE OR REPLACE TEMPORARY TABLE temp_stream_batch AS 
SELECT 
    event_id,
    event_timestamp,
    payload:user_id::INT AS user_id,
    payload:action::STRING AS action_type,
    payload:amount::FLOAT AS transaction_amount,
    payload:location::STRING AS location
FROM WEBHOOK_STREAM
WHERE METADATA$ACTION = 'INSERT';

-- 3. Route normal/clean transactions to CLEANED_TRANSACTIONS
INSERT INTO CLEANED_TRANSACTIONS (event_id, event_timestamp, user_id, action_type, transaction_amount, location)
SELECT event_id, event_timestamp, user_id, action_type, transaction_amount, location
FROM temp_stream_batch
WHERE transaction_amount <= 3000.00;

-- 4. Route high-risk or unknown location transactions to FRAUD_ALERTS
INSERT INTO FRAUD_ALERTS (event_id, event_timestamp, user_id, transaction_amount, location, risk_reason)
SELECT 
    event_id,
    event_timestamp,
    user_id,
    transaction_amount,
    location,
    CASE 
        WHEN transaction_amount > 3000.00 THEN 'High Value Transaction Threshold Exceeded'
        WHEN location = 'Unknown' THEN 'Unrecognized Geolocation'
        ELSE 'Suspicious Activity'
    END AS risk_reason
FROM temp_stream_batch
WHERE transaction_amount > 3000.00 OR location = 'Unknown';

-- 5. View your populated tables!
SELECT * FROM CLEANED_TRANSACTIONS;
SELECT * FROM FRAUD_ALERTS;
