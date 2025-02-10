# Environment Setup Guide

This document provides a step-by-step guide to setting up the PySpark environment for retail database analysis.

## Step 1: Install Required Libraries
```bash
pip install pyspark pandas psycopg2
```

## Step 2: Download Dataset
Download data from:
```
https://github.com/erkansirin78/datasets/tree/master/retail_db
```

Move CSV files to your working directory.

## Step 3: Configure PostgreSQL
1. Ensure PostgreSQL is installed and running.
2. Create a database `test1`:
   ```sql
   CREATE DATABASE test1;
   ```
3. Provide correct connection details in the script.

## Step 4: Run Analysis Script
Execute:
```bash
python retail_db_analysis.py
```
