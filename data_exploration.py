#!/usr/bin/env python3
"""
Data Exploration Script for Sales Data
This script explores the CSV files to understand data structure, quality, and potential issues.
"""
import os
import pandas as pd
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def explore_with_pandas():
    """Quick exploration using Pandas for initial insights"""
    print("=" * 60)
    print("DATA EXPLORATION WITH PANDAS")
    print("=" * 60)
    
    sales_data_path = Path('Sales_Data')
    csv_files = list(sales_data_path.glob('*.csv'))
    
    print(f"Found {len(csv_files)} CSV files")
    print("\nFile Details:")
    print("-" * 40)
    
    total_size = 0
    for file in csv_files:
        size_kb = file.stat().st_size / 1024
        total_size += size_kb
        print(f"{file.name:<25} | {size_kb:>8.1f} KB")
    
    print(f"{'Total Size':<25} | {total_size:>8.1f} KB")
    
    # Sample first file for schema understanding
    sample_file = csv_files[0]
    print(f"\n🔍 ANALYZING SAMPLE FILE: {sample_file.name}")
    print("-" * 50)
    
    try:
        # Read sample with different configurations to handle potential issues
        df = pd.DataFrame()

        for file in os.listdir(sales_data_path):
            if file.endswith('.csv'):
                df_file = pd.read_csv(sales_data_path / file, low_memory=False)
                df = pd.concat([df, df_file], ignore_index=True)
        
        print(f"Sample shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print(f"\nData types:")
        for col, dtype in df.dtypes.items():
            print(f"  {col:<20}: {dtype}")
        
        
        print(f"\nNull values per column:")
        null_counts = df.isnull().sum()
        for col, count in null_counts.items():
            if count > 0:
                print(f"  {col:<20}: {count} nulls")
        
        if null_counts.sum() == 0:
            print("  No null values found in sample!")

        null_df = df[df.isnull().any(axis=1)]
        null_df.to_csv(os.path.join('exploration/null_values_sample.csv'), index=False)
        print()


        # Check for duplicates in sample
        duplicates = df.duplicated().sum()
        print(f"\nDuplicates in sample: {duplicates}")
        
        # Analyze Order Date format
        if 'Order Date' in df.columns:
            print(f"\nOrder Date Analysis:")
            print(f"  Sample dates: {df['Order Date'].head(3).tolist()}")
            print(f"  Unique date formats detected: {df['Order Date'].nunique()} unique values in sample")
        
        duplicated_df = df[df.duplicated(keep=False)]
        duplicated_df.to_csv(os.path.join('exploration/duplicated_rows.csv'), index=False)

        return df.columns.tolist(), df.dtypes.to_dict()
        
    except Exception as e:
        logger.error(f"Error reading {sample_file.name}: {str(e)}")
        return None, None

def define_spark_schema(column_names):
    """Define optimized Spark schema based on known structure"""
    print("\n" + "=" * 60)
    print("SPARK SCHEMA DEFINITION")
    print("=" * 60)
    
    # Define schema based on expected columns
    schema = StructType([
        StructField("Order ID", IntegerType(), True),
        StructField("Product", StringType(), True),
        StructField("Quantity Ordered", IntegerType(), True),
        StructField("Price Each", DoubleType(), True),
        StructField("Order Date", StringType(), True),  # We'll convert to timestamp later
        StructField("Purchase Address", StringType(), True)
    ])
    
    print("Defined Spark Schema:")
    for field in schema.fields:
        nullable = "nullable" if field.nullable else "not null"
        print(f"  {field.name:<20}: {field.dataType.simpleString():<10} ({nullable})")
    
    return schema

def test_spark_schema():
    """Test reading one file with PySpark using defined schema"""
    print("\n" + "=" * 60)
    print("SPARK SCHEMA VALIDATION")
    print("=" * 60)
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("DataExploration") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")  # Reduce log noise
    
    try:
        sales_data_path = Path('Sales_Data')
        csv_files = list(sales_data_path.glob('*.csv'))
        sample_file = str(csv_files[0])
        
        print(f"Testing PySpark read on: {Path(sample_file).name}")
        
        # Read with inferred schema first
        df_inferred = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(sample_file)
        
        print(f"\nInferred Schema:")
        df_inferred.printSchema()
        
        print(f"\nRow count: {df_inferred.count()}")
        print(f"Columns: {df_inferred.columns}")
        
        # Show sample data
        print(f"\nSample data (first 5 rows):")
        df_inferred.show(5, truncate=False)
        
        # Check for potential duplicates based on Order ID
        print(f"\nDuplicate Analysis:")
        total_rows = df_inferred.count()
        unique_order_ids = df_inferred.select("Order ID").distinct().count()
        print(f"  Total rows: {total_rows}")
        print(f"  Unique Order IDs: {unique_order_ids}")
        print(f"  Potential duplicates: {total_rows - unique_order_ids}")
        
        # Analyze date formats
        if "Order Date" in df_inferred.columns:
            print(f"\nDate Format Analysis:")
            date_samples = df_inferred.select("Order Date").distinct().limit(5).collect()
            for row in date_samples:
                print(f"  Sample date: {row['Order Date']}")
        
        spark.stop()
        return True
        
    except Exception as e:
        logger.error(f"Error in Spark schema validation: {str(e)}")
        spark.stop()
        return False

def analyze_all_files_consistency():
    """Check if all CSV files have consistent structure"""
    print("\n" + "=" * 60)
    print("FILE CONSISTENCY ANALYSIS")
    print("=" * 60)
    
    sales_data_path = Path('Sales_Data')
    csv_files = list(sales_data_path.glob('*.csv'))
    
    file_schemas = {}
    total_rows = 0
    
    for file in csv_files[:5]:  # Check first 5 files for speed
        try:
            df = pd.read_csv(file, nrows=10)  # Just header + few rows
            file_schemas[file.name] = {
                'columns': list(df.columns),
                'shape': df.shape,
                'dtypes': df.dtypes.to_dict()
            }
            
            # Get full row count for this file
            full_df = pd.read_csv(file)
            total_rows += len(full_df)
            print(f"{file.name:<25} | Columns: {len(df.columns)} | Rows: {len(full_df):>6}")
            
        except Exception as e:
            logger.error(f"Error reading {file.name}: {str(e)}")
    
    print(f"\nTotal rows across all files: {total_rows:,}")
    
    # Check schema consistency
    first_file_cols = list(file_schemas.values())[0]['columns']
    consistent = True
    
    for filename, schema_info in file_schemas.items():
        if schema_info['columns'] != first_file_cols:
            print(f"⚠️  Schema mismatch in {filename}")
            consistent = False
    
    if consistent:
        print("✅ All checked files have consistent schema!")
    
    return consistent, total_rows


def check_columns_type():
    """Check if all columns can be turned into desired data types"""
    print("\n" + "=" * 50)
    print("COLUMN TYPE CONSISTENCY CHECK")
    print("=" * 50)

    types = {
        'Product': 'string',
        'Quantity Ordered': 'int',
        'Price Each': 'float',
        'Order Date': 'datetime',
    }

    def can_cast_column(series, target_type):
        try:
            if target_type == 'string':
                series.astype('string')
            elif target_type == 'int':
                pd.to_numeric(series, errors='raise', downcast='integer')
            elif target_type == 'float':
                pd.to_numeric(series, errors='raise')
            elif target_type == 'datetime':
                pd.to_datetime(series, errors='raise')
            else:
                raise ValueError(f"Unknown type: {target_type}")
            return True
        except Exception:
            return False


    df = pd.DataFrame()
    for file in os.listdir('Sales_Data'):
        if file.endswith('.csv'):
            df_file = pd.read_csv(Path('Sales_Data') / file, low_memory=False)
            df = pd.concat([df, df_file], ignore_index=True)

    df = df[df.columns.dropna()]  # Drop columns with all NaN values
    df = df.drop_duplicates(keep='first')  # Remove duplicates
    df = df[pd.to_numeric(df['Order ID'], errors='coerce').notna()]  # Ensure 'Order ID' is numeric        

    results = {col: can_cast_column(df[col], t) for col,t in types.items()}
    
    #print(results)
    print("\nColumn Type Check Results:")
    for col, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{col:<20}: {status}")   

        

        





def main():
    """Run complete data exploration"""
    print("🔍 STARTING DATA EXPLORATION")
    print("=" * 60)
    
    # Step 1: Pandas exploration
    columns, dtypes = explore_with_pandas()
    
    if columns is None:
        print("❌ Failed to read sample data. Please check your CSV files.")
        return False
    
    # Step 2: Define Spark schema
    schema = define_spark_schema(columns)
    
    # Step 3: Test with Spark
    spark_success = test_spark_schema()
    
    # Step 4: Check consistency
    consistent, total_rows = analyze_all_files_consistency()

    # Step 5: Check column types
    check_columns_type()
    
    
    print(f"\n🎯 EXPLORATION COMPLETE!")
    print(f"   Files are {'consistent' if consistent else 'inconsistent'}")
    print(f"   Spark test {'passed' if spark_success else 'failed'}")
    print(f"   Ready to build main pipeline: {'✅ YES' if consistent and spark_success else '❌ ISSUES FOUND'}")
    
    return consistent and spark_success

if __name__ == "__main__":
    success = main()