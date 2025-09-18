# Serverless Assignment

What is Serverless?


# Table of Contents

# Architecture

figure chaiyo

# Data Pipeline

figure chaiyo

## Data Ingestion

### Dataset
The dataset is an OHCV stock data of companies of US taken from [twelvedata](https://twelvedata.com/stocks). The `json` data is manually ingested to the desired s3 bucket's `inputs/` folder which triggers the pipeline.

### Data Ingestor Lambda

[Source file](infra/modules/ingestor/src/data_ingestor.py)
The Data Ingestor Lambda serves as the entry point of the serverless data pipeline. When a file is uploaded to the `inputs/` folder, this Lambda validates the data, separates valid records from invalid ones, and stores them in appropriate S3 locations.

The `validate_record()` function checks each stock record for required fields like datetime, open, high, low, close, and volume prices. It ensures data types are correct and performs basic sanity checks on OHLC values. The `process_stock_data()` function orchestrates the validation process, flattening the nested JSON structure and collecting valid and invalid records separately.

To prevent duplicate processing, the Lambda uses S3 ETag-based deduplication through marker files stored in the `processed/hashes/` prefix. The `write_to_s3()` function handles output operations, writing valid records to the `processed/` folder and rejected records to the `rejects/` folder in JSON Lines format.

The main `lambda_handler()` coordinates the entire process, from downloading the input file to publishing CloudWatch metrics about rejection rates under the `mejan-pipeline` namespace. Upon successful processing, it publishes an `IngestorCompleted` event to EventBridge to trigger downstream components like the analyzer Lambda.

The Lambda implements comprehensive error handling and structured JSON logging with correlation IDs for traceability across the pipeline. All operations are designed to be idempotent, using timestamp-based naming for output files to handle retries gracefully.

#### Idempotency

## Data Analyzer Lambda

Triggered by EventBridge events from the ingestor, this Lambda performs statistical computations, detects anomalies, and uses Amazon Bedrock to produce actionable trading insights.

The analyzer begins with the `compute_metrics()` function which calculates key financial indicators:

- Trend direction
- Momentum over recent periods
- Volatility using standard deviation 
- Percentage changes

It also identifies anomalies such as unusual trading volumes exceeding 1.5 times the average or sharp price movements beyond 5% thresholds. The `compute_aggregates()` function synthesizes market-wide statistics across all symbols, determining overall market sentiment, identifying top gainers and losers, and summarizing turbulence levels.

The core intelligence comes from `call_bedrock()` which constructs detailed prompts for the Amazon **Nova Lite** model, requesting both per-symbol analysis and executive summaries. The function implements retry logic with up to 5 attempts and publishes CloudWatch metrics when retry limits are exceeded.

Results are persisted through `store_analysis()` which writes analysis results to the DynamoDB table specified by the `TABLE_NAME` environment variable. The stored records include symbol insights, aggregate statistics, executive summaries, and row count metadata from the original ingestor processing. 


### Cost Breakdown and comparison

## Notifier

The Email Notifier Lambda completes the pipeline by sending formatted analysis reports via Amazon SES whenever new analysis data is written to DynamoDB. Triggered by **DynamoDB Streams** on `INSERT` events, this Lambda transforms the stored analysis into professional HTML email reports for stakeholders.

Each successful analysis triggers an immediate email notification with correlation IDs for complete pipeline traceability. 

# CI-CD Pipeline



# Git-back sync

# JSON logging (structured)


<!-- ## S3 lifecyle -->

