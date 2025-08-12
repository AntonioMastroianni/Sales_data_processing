# 🛒 Sales Data Exploration & Processing Pipeline (PySpark + Pandas)

## 📌 Overview
This project is a **Windows-compatible PySpark data pipeline** designed to explore, clean, and process large volumes of sales data from CSV files into an optimized **Parquet format**.  
It includes:
- Data exploration with **Pandas** and **PySpark**
- Schema validation and type checking
- Duplicate detection and removal
- Data quality checks and summary reports
- Partitioned Parquet output for efficient analytics

The pipeline is optimized for **Windows environments**, with specific Spark and Hadoop configurations to prevent common native I/O issues.

---

## ⚙️ Requirements
Before running the scripts, ensure you have:
- **Python** 3.7+
- **Java** JDK 8 or higher
- **PySpark** (`pip install pyspark`)
- **Pandas** (`pip install pandas`)
- A folder named `Sales_Data` containing CSV sales files

---

## 🚀 Setup & Usage

### 1️⃣ Check Environment
Run:
```bash
python environment_check.py
```

### 2️⃣ Explore Data
Run:
```bash
python data_exploration.py
```

### 3️⃣ Run the Processing Pipeline
Run:
```bash
python data_pipeline.py
```
