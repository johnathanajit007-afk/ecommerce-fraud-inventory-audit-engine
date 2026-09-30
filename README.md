# 🚀 E-Commerce Fraud & Inventory Audit Engine (Snowflake)

A production-grade, end-to-end data engineering pipeline built entirely in **Snowflake** to ingest semi-structured webhooks, track real-time changes using Change Data Capture (CDC), and automatically route transactions into clean operational tables and security fraud alerts.

---

## 🏗️ Architecture & Workflow

1. **Semi-Structured Ingestion:** Ingests live webhook data containing nested JSON payloads into a raw landing table utilizing Snowflake's `VARIANT` data type.
2. **Change Data Capture (CDC):** Utilizes Snowflake `STREAM` objects to track delta changes (`INSERT` operations) efficiently without expensive full-table scans.
3. **Staging & Conditional Routing:** Safely captures stream data into a temporary staging table to prevent offset consumption traps, routing transactions based on strict business logic:
   * **Clean Transactions:** Threshold $\le$ $3,000.00 route to `CLEANED_TRANSACTIONS`.
   * **Fraud Alerts:** High-value transactions (> $3,000.00) or unrecognized geolocations (`Unknown`) automatically route to `FRAUD_ALERTS` with explicit risk reasons.
4. **Automation:** Powered by automated Snowflake `TASK` orchestration and auto-suspending virtual warehouses for cost efficiency.

---

## 🛠️ Tech Stack & Concepts
* **Cloud Data Warehouse:** Snowflake (`ACCOUNTADMIN`, custom Virtual Warehouses)
* **Data Types & Parsing:** `VARIANT`, dot-notation, and explicit type casting (`::FLOAT`, `::INT`, `::STRING`)
* **CDC & Orchestration:** Streams (`METADATA$ACTION`), Temporary Staging patterns, and scheduled Tasks

---

## 📂 Repository Structure
* `ecommerce_fraud_audit_engine.sql` — Complete end-to-end SQL script containing database setup, stream configuration, routing logic, and test cases.
