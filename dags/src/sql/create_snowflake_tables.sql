-- Snowflake schema for financial data warehouse

CREATE DATABASE IF NOT EXISTS FINANCIAL_DW;
CREATE SCHEMA IF NOT EXISTS FINANCIAL_DW.GOLD;

CREATE TABLE IF NOT EXISTS FINANCIAL_DW.GOLD.DAILY_TRANSACTIONS (
    transaction_id      VARCHAR(50)     NOT NULL,
    transaction_date    DATE            NOT NULL,
    amount_usd          DECIMAL(18, 2),
    category            VARCHAR(100),
    account_id          VARCHAR(50),
    loaded_at           TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE VIEW FINANCIAL_DW.GOLD.DAILY_SUMMARY AS
SELECT
    transaction_date,
    category,
    COUNT(*)            AS total_transactions,
    SUM(amount_usd)     AS total_amount,
    AVG(amount_usd)     AS avg_amount
FROM FINANCIAL_DW.GOLD.DAILY_TRANSACTIONS
GROUP BY transaction_date, category;
