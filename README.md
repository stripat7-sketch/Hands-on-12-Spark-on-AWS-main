# Serverless Spark ETL Pipeline on AWS

## Project Overview
This project implements a fully automated, event-driven serverless data pipeline on AWS. The pipeline automatically ingests raw CSV product review data, processes it using a Spark ETL job, runs analytical SQL queries, and saves the aggregated results back to S3.

## AWS Services Used
| Service | Purpose |
|---------|---------|
| S3 | Data lake storage for raw and processed data |
| Lambda | Event-driven trigger for Glue job |
| Glue | Serverless Spark ETL processing |
| IAM | Security and permissions |

## Implementation Steps

### 1. S3 Buckets Created
| Bucket Name | Purpose |
|-------------|---------|
| `handsonfinallandingqwerty` | Raw data landing zone |
| `handsonfinalprocessedqwerty` | Processed results storage |

### 2. IAM Role Configuration
- **Role Name:** `AWSGlueServiceRole-Reviews`
- **Attached Policies:**
  - `AWSGlueServiceRole` (Glue permissions)
  - `AmazonS3FullAccess` (S3 read/write permissions)

### 3. Glue ETL Job
- **Job Name:** `process_reviews_job`
- **Type:** Spark script editor
- **Glue Version:** 4.0
- **Workers:** 2 (G.1X)
- **Status:** ✅ Succeeded

### 4. Lambda Trigger Function
- **Function Name:** `start_glue_job_trigger`
- **Runtime:** Python 3.11
- **Trigger:** S3 (handsonfinallandingqwerty - All object create events)

## Data Processing Steps

### Input Data (reviews.csv)
| Column | Description |
|--------|-------------|
| review_id | Unique review identifier |
| product_id | Product identifier |
| customer_id | Customer identifier |
| rating | Rating (1-5 stars) |
| review_date | Date of review |
| review_text | Text content |

### Transformations Applied
1. Fill null ratings with 0
2. Convert review_date to proper date format
3. Fill null review_text with "No review text"
4. Fill null customer_id with "unknown"
5. Convert product_id to uppercase

## SQL Queries Implemented

### Query 1: Product Performance
Calculates average rating and review count for each product.
**Output:** `product_performance/`

### Query 2: Daily Review Counts
Shows total reviews submitted per day.
**Output:** `daily_review_counts/`

### Query 3: Top 5 Most Active Customers
Identifies top 5 customers by number of reviews.
**Output:** `top_5_customers/`

### Query 4: Rating Distribution
Shows count of reviews for each rating (1-5 stars).
**Output:** `rating_distribution/`

## Results Summary

### Query 1 - Product Performance
[Open your CSV and write a summary]
- Highest rated product: `[product_id]` with `[X]` average rating
- Most reviewed product: `[product_id]` with `[X]` reviews

### Query 2 - Daily Review Counts
[Write a summary]
- Total days with reviews: `[X]`
- Highest review day: `[date]` with `[X]` reviews

### Query 3 - Top 5 Customers
[Write a summary]
- Most active customer: `[customer_id]` with `[X]` reviews

### Query 4 - Rating Distribution
[Write a summary]
- Most common rating: `[X]` stars with `[X]` reviews

## Challenges & Solutions

### Challenge 1: Access Denied Error
**Issue:** Glue job failed with S3 permission errors.
**Solution:** Added `AmazonS3FullAccess` policy to the IAM role.

### Challenge 2: Concurrent Runs Exceeded
**Issue:** Multiple Glue jobs running simultaneously.
**Solution:** Waited for previous jobs to complete and reduced worker count to 1.

### Challenge 3: Bucket Name Mismatch
**Issue:** Script used generic bucket names but actual buckets had "qwerty" suffix.
**Solution:** Updated bucket names in the Glue script to match actual names.

## Cost Analysis (Free Tier)
| Service | Usage | Cost |
|---------|-------|------|
| S3 Storage | ~1 MB | $0.00 |
| Glue ETL | 1.5 minutes | $0.00 |
| Lambda | 1 invocation | $0.00 |
| **Total** | | **$0.00** |

## Screenshots

| # | Description | Screenshot |
|---|-------------|------------|
| 1 | Glue Job Success | ![Glue Success]() |
| 2 | S3 Buckets | ![S3 Buckets]() |
| 3 | S3 Results | ![S3 Results]() |
| 4 | IAM Role | ![IAM Role]() |
| 5 | Lambda Trigger | ![Lambda Trigger]() |

## Cleanup Instructions
To avoid future charges:
1. Empty and delete both S3 buckets
2. Delete Glue job `process_reviews_job`
3. Delete Lambda function `start_glue_job_trigger`
4. Delete IAM role `AWSGlueServiceRole-Reviews`
