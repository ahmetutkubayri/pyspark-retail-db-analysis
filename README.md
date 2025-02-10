# Retail Database Analysis with PySpark

This project analyzes a retail database using PySpark, covering data exploration, transformations, and writing results in Parquet and PostgreSQL.

## 🚀 Features
- Count distinct order items and row counts
- Identify most canceled products and categories by sales
- Analyze sales trends by month and day of the week
- Perform full table joins and store in PostgreSQL

## 📁 Repository Structure
- `retail_db_analysis.py` → Python script for data processing
- `setup.md` → Guide for setting up PySpark and PostgreSQL
- `solutions.md` → Explanation of analysis steps and outputs

## 🔧 Prerequisites
- Python 3 with PySpark installed
- PostgreSQL database (`test1`)
- Dataset from:
  ```
  https://github.com/erkansirin78/datasets/tree/master/retail_db
  ```

## 🏗️ Running the Analysis
1. Install required libraries:
   ```bash
   pip install pyspark pandas psycopg2
   ```

2. Run the script:
   ```bash
   python retail_db_analysis.py
   ```

3. The cleaned dataset will be stored in Parquet and PostgreSQL.
