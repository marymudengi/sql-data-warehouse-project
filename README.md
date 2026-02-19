# sql-data-warehouse-project

SQL Data Warehouse Implementation Project
Overview
Designed and implemented a layered Data Warehouse using the Medallion Architecture (Bronze, Silver, Gold) to transform raw operational data into reliable, analytics-ready datasets.
The objective was not just to store data, but to improve data quality, consistency, and usability for reporting.
Business Problem
Operational system data could not be reliably used for reporting. Users experienced:
Conflicting report numbers
Manual Excel corrections
Missing values
Duplicate records
Hard-to-interpret coded fields
Goal:
Create a structured data platform that produces clean, trustworthy reporting datasets and a single source of truth.
Architecture
I created a database named DataWarehouse with three schemas:
Layer
Purpose
bronze
Raw source data
silver
Cleaned and standardized data
gold
Business reporting models


My Approach
1. Understanding the Source Data
Before building the warehouse, I analyzed the raw data:
Reviewed source files
Identified entities and primary keys
Documented field meanings
Defined naming conventions
Bronze Layer — Data Ingestion
The Bronze layer stores data exactly as received to preserve history.
Actions performed
Connected to CSV and Excel sources
Handled structured vs unstructured sheets
Implemented bulk load ingestion
Skipped headers and defined delimiters
Performed schema and completeness validation
This layer serves as the system-of-record snapshot.
Silver Layer — Data Cleaning & Standardization
This layer focused on improving data quality and usability.
Metadata Tracking
Added traceability fields:
create_date
update_date
source_system
file_location
Deduplication & Record Versioning
Using SQL window functions:
Identified latest record per entity
Flagged active records
Prevented duplicate reporting entries
Data Quality Improvements
Trimmed leading/trailing spaces
Converted coded values (F/M → Female/Male)
Standardized categories
Filled missing values using business rules
Removed duplicate records
Normalized formats across datasets
Gold Layer — Reporting Models
Created analytics-ready structures:
Fact tables
Dimension tables
Aggregated datasets
BI tools could now connect directly without manual cleanup.
Automation
Automated ingestion and transformation using stored procedures to:
Standardize repeatable processing
Enable scheduled refreshes
Ensure consistent data loads
Version Control & Documentation
All scripts stored in Git
Transformations documented
Repeatable deployment process created
Outcome
The warehouse delivered:
Trusted single source of truth
Consistent business definitions
Reduced manual corrections
Cleaner reports
Faster analytics development
Key Learning
Most reporting issues originate from data quality rather than tooling.
A layered warehouse preserves raw data while progressively improving trust and usability.
