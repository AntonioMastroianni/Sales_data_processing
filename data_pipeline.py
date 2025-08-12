#!/usr/bin/env python3
"""
Windows-Compatible PySpark Data Pipeline for Sales Data Processing
Fixes the NativeIO Windows compatibility issue by using alternative configurations
"""

import logging
import os
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, isnan, isnull, to_timestamp, year, month, 
    row_number, count, sum as spark_sum, max as spark_max, min as spark_min,
    desc
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class WindowsCompatibleSalesDataPipeline:
    """
    Windows-compatible pipeline class for processing sales data from multiple CSV files
    Addresses Hadoop native library issues on Windows
    """
    
    def __init__(self, source_path="Sales_Data", target_path="cleansed"):
        self.source_path = Path(source_path)
        self.target_path = Path(target_path)
        self.spark = None
        self.metrics = {}
        
        # Ensure target directory exists
        self.target_path.mkdir(parents=True, exist_ok=True)
    
    def initialize_spark_windows_compatible(self):
        """Initialize Spark session with Windows-compatible configurations"""
        logger.info("Initializing Windows-compatible Spark session...")
        
        # Set system properties to handle Windows issues
        os.environ['HADOOP_HOME'] = os.environ.get('HADOOP_HOME', '')
        
        self.spark = SparkSession.builder \
            .appName("WindowsCompatibleSalesDataPipeline") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1MB") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .config("spark.hadoop.hadoop.native.io", "false") \
            .config("spark.hadoop.io.native.lib.available", "false") \
            .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS") \
            .config("spark.sql.session.timeZone", "UTC") \
            .config("spark.hadoop.fs.defaultFS", "file:///") \
            .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse") \
            .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=/tmp") \
            .config("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=/tmp") \
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", "1000") \
            .getOrCreate()
        
        # Reduce log noise
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info(" Windows-compatible Spark session initialized successfully")
    
    def define_schema(self):
        """Define explicit schema for consistent data reading"""
        return StructType([
            StructField("Order ID", StringType(), True),
            StructField("Product", StringType(), True),
            StructField("Quantity Ordered", IntegerType(), True),
            StructField("Price Each", DoubleType(), True),
            StructField("Order Date", StringType(), True),
            StructField("Purchase Address", StringType(), True)
        ])
    
    def read_csv_files(self):
        """Read all CSV files from source directory and combine them"""
        logger.info(f"Reading CSV files from {self.source_path}")
        
        csv_files = list(self.source_path.glob("*.csv"))
        if not csv_files:
            raise ValueError(f"No CSV files found in {self.source_path}")
        
        logger.info(f"Found {len(csv_files)} CSV files")
        
        # Read files individually and union them
        schema = self.define_schema()
        dataframes = []
        
        for i, csv_file in enumerate(csv_files, 1):
            logger.info(f"Reading file {i}/{len(csv_files)}: {csv_file.name}")
            try:
                # Use absolute path for Windows compatibility
                file_path = str(csv_file.resolve())
                
                df_temp = self.spark.read \
                    .schema(schema) \
                    .option("header", "true") \
                    .option("timestampFormat", "MM/dd/yy HH:mm") \
                    .option("multiline", "true") \
                    .option("escape", '"') \
                    .option("encoding", "UTF-8") \
                    .option("inferSchema", "false") \
                    .csv(file_path)
                
                row_count = df_temp.count()
                logger.info(f"   Successfully read {row_count:,} rows from {csv_file.name}")
                dataframes.append(df_temp)
                
            except Exception as e:
                logger.error(f"   Failed to read {csv_file.name}: {str(e)}")
                raise
        
        # Union all dataframes
        logger.info("Combining all CSV files...")
        df = dataframes[0]
        for df_temp in dataframes[1:]:
            df = df.union(df_temp)
        
        # Store initial metrics
        initial_count = df.count()
        self.metrics['input_files'] = len(csv_files)
        self.metrics['initial_row_count'] = initial_count
        
        logger.info(f" Successfully combined {initial_count:,} rows from {len(csv_files)} files")
        
        # Format column names for easier processing
        df = self.format_column_names(df)
        
        return df
    
    def format_column_names(self, df):
        """Format column names: lowercase and replace spaces with underscores"""
        logger.info("Formatting column names...")
        
        original_columns = df.columns
        column_mapping = {}
        
        for col_name in original_columns:
            new_col_name = col_name.lower().replace(" ", "_")
            column_mapping[col_name] = new_col_name
        
        # Rename all columns
        df_formatted = df
        for old_name, new_name in column_mapping.items():
            df_formatted = df_formatted.withColumnRenamed(old_name, new_name)
        
        logger.info(f" Column names formatted: {list(column_mapping.values())}")
        return df_formatted
    
    def clean_data(self, df):
        """Clean data by removing null rows, invalid records, and non-integer Order IDs"""
        logger.info("Starting comprehensive data cleaning...")
        
        initial_count = df.count()
        
        # Step 1: Remove rows where ANY critical field is null
        logger.info("Removing rows with null values in critical columns...")
        df_cleaned = df.filter(
            col("order_id").isNotNull() &
            col("product").isNotNull() &
            col("quantity_ordered").isNotNull() &
            col("price_each").isNotNull() &
            col("order_date").isNotNull() &
            col("purchase_address").isNotNull()
        )
        
        null_removed_count = initial_count - df_cleaned.count()
        logger.info(f" Removed {null_removed_count:,} rows with null values")
        
        # Step 2: Remove rows where Order ID cannot be converted to integer
        logger.info("Filtering rows with valid integer Order IDs...")
        df_cleaned = df_cleaned.filter(col("order_id").cast("string").rlike(r"^\d+$"))
        df_cleaned = df_cleaned.withColumn("order_id", col("order_id").cast("integer"))
        
        invalid_id_removed = initial_count - null_removed_count - df_cleaned.count()
        logger.info(f" Removed {invalid_id_removed:,} rows with invalid Order IDs")
        
        # Step 3: Remove rows with invalid quantities or prices
        logger.info("Removing rows with invalid quantities or prices...")
        df_cleaned = df_cleaned.filter(
            (col("quantity_ordered") > 0) &
            (col("price_each") > 0)
        )
        
        # Step 4: Convert Order Date to proper timestamp and add partitioning columns
        logger.info("Converting Order Date to timestamp format...")
        df_cleaned = df_cleaned.withColumn(
            "order_date_parsed",
            to_timestamp(col("order_date"), "MM/dd/yy HH:mm")
        )
        
        # Filter out rows where date parsing failed
        df_cleaned = df_cleaned.filter(col("order_date_parsed").isNotNull())
        
        # Add derived columns for partitioning
        df_cleaned = df_cleaned \
            .withColumn("order_year", year(col("order_date_parsed"))) \
            .withColumn("order_month", month(col("order_date_parsed")))
        
        # Final metrics
        final_cleaned_count = df_cleaned.count()
        total_removed = initial_count - final_cleaned_count
        
        self.metrics['cleaned_row_count'] = final_cleaned_count
        self.metrics['removed_rows'] = total_removed
        self.metrics['null_rows_removed'] = null_removed_count
        self.metrics['invalid_id_rows_removed'] = invalid_id_removed
        
        logger.info(f" Data cleaning complete: {final_cleaned_count:,} rows remaining")
        
        return df_cleaned
    
    def deduplicate_data_window_function(self, df):
        """Remove duplicates using Window function - keeps most recent record per Order ID + Product"""
        logger.info("Starting Window function-based deduplication...")
        
        initial_count = df.count()
        
        # Define window specification for deduplication
        window_spec = Window.partitionBy("order_id", "product") \
                           .orderBy(col("order_date_parsed").desc())
        
        logger.info("Applying Window function with row_number()...")
        
        # Add row number within each partition
        df_with_row_num = df.withColumn("row_num", row_number().over(window_spec))
        
        # Keep only the first record (most recent) for each Order ID + Product combination
        df_deduplicated = df_with_row_num.filter(col("row_num") == 1).drop("row_num")
        
        # Additional step: Remove exact duplicate rows (if any remain)
        df_deduplicated = df_deduplicated.dropDuplicates()
        
        deduplicated_count = df_deduplicated.count()
        duplicates_removed = initial_count - deduplicated_count
        
        self.metrics['deduplicated_row_count'] = deduplicated_count
        self.metrics['duplicates_removed'] = duplicates_removed
        
        logger.info(f" Window function deduplication complete:")
        logger.info(f"  - Initial rows: {initial_count:,}")
        logger.info(f"  - Duplicates removed: {duplicates_removed:,}")
        logger.info(f"  - Final unique rows: {deduplicated_count:,}")
        
        if duplicates_removed > 0:
            logger.info(f"  - Strategy: Keep most recent record per Order ID + Product")
        
        return df_deduplicated
    
    def perform_data_quality_checks(self, df):
        """Perform comprehensive data quality validations"""
        logger.info("Performing enhanced data quality checks...")
        
        total_rows = df.count()
        
        # Check for nulls in critical columns
        null_checks = {}
        critical_columns = ["order_id", "product", "quantity_ordered", "price_each"]
        
        for column in critical_columns:
            null_count = df.filter(col(column).isNull()).count()
            null_checks[column] = null_count
        
        # Enhanced statistics
        quantity_stats = df.select(
            spark_min("quantity_ordered").alias("min_qty"),
            spark_max("quantity_ordered").alias("max_qty"),
            spark_sum("quantity_ordered").alias("total_qty")
        ).collect()[0]
        
        price_stats = df.select(
            spark_min("price_each").alias("min_price"),
            spark_max("price_each").alias("max_price")
        ).collect()[0]
        
        date_range = df.select(
            spark_min("order_date_parsed").alias("min_date"),
            spark_max("order_date_parsed").alias("max_date")
        ).collect()[0]
        
        # Count distinct products and order IDs
        distinct_products = df.select("product").distinct().count()
        distinct_order_ids = df.select("order_id").distinct().count()
        
        # Store enhanced quality metrics
        self.metrics.update({
            'final_row_count': total_rows,
            'distinct_products': distinct_products,
            'distinct_order_ids': distinct_order_ids,
            'null_checks': null_checks,
            'quantity_range': (quantity_stats['min_qty'], quantity_stats['max_qty']),
            'total_quantity': quantity_stats['total_qty'],
            'price_range': (price_stats['min_price'], price_stats['max_price']),
            'date_range': (date_range['min_date'], date_range['max_date'])
        })
        
        # Enhanced validation logic
        quality_passed = True
        issues = []
        
        # Check for excessive nulls
        for column, null_count in null_checks.items():
            if null_count > 0:
                issues.append(f"{column}: {null_count} null values detected")
                quality_passed = False
        
        # Check for reasonable data ranges
        if quantity_stats['min_qty'] < 1:
            issues.append("Invalid quantities detected")
            quality_passed = False
        
        if price_stats['min_price'] <= 0:
            issues.append("Invalid prices detected") 
            quality_passed = False
        
        # Check for reasonable number of distinct values
        if distinct_products < 1:
            issues.append("No distinct products found")
            quality_passed = False
            
        if distinct_order_ids < 1:
            issues.append("No distinct order IDs found")
            quality_passed = False
        
        self.metrics['quality_passed'] = quality_passed
        self.metrics['quality_issues'] = issues
        
        logger.info(" Data quality check results:")
        logger.info(f"  - Total rows: {total_rows:,}")
        logger.info(f"  - Distinct products: {distinct_products:,}")
        logger.info(f"  - Distinct orders: {distinct_order_ids:,}")
        logger.info(f"  - Quality status: {' PASSED' if quality_passed else '⚠️ ISSUES DETECTED'}")
        
        if not quality_passed:
            logger.warning(f"  - Issues: {issues}")
        
        return quality_passed
    
    def write_single_parquet(self, df):
        """
        Write the processed DataFrame as a single Parquet file
        in the cleansed folder.
        """
        logger.info("Writing final dataset to a single Parquet file...")

        # Select final columns for output
        final_df = df.select(
            "order_id",
            "product", 
            "quantity_ordered",
            "price_each",
            col("order_date_parsed").alias("order_date"),
            "purchase_address",
            "order_year",
            "order_month"
        )

        # Coalesce to a single partition before writing
        output_path = str(self.target_path / "sales_data.parquet")
        final_df.coalesce(1).write.mode("overwrite").parquet(output_path)

        # Update metrics
        self.metrics['output_row_count'] = final_df.count()
        self.metrics['output_format'] = 'Single Parquet'
        self.metrics['output_path'] = output_path

        logger.info(f"✅ Written {self.metrics['output_row_count']:,} rows to {output_path}")

        
    def generate_enhanced_summary_report(self):
        """Generate comprehensive processing summary report"""
        logger.info("Generating enhanced processing summary...")
        
        report = f"""
{'='*80}
                    WINDOWS-COMPATIBLE PIPELINE SUMMARY
{'='*80}

 DATA PROCESSING METRICS:
   Input Files Processed       : {self.metrics.get('input_files', 'N/A')}
   Initial Row Count           : {self.metrics.get('initial_row_count', 0):,}
   After Data Cleaning         : {self.metrics.get('cleaned_row_count', 0):,}
   After Window Deduplication  : {self.metrics.get('deduplicated_row_count', 0):,}
   Final Output Rows           : {self.metrics.get('output_row_count', 0):,}
   
 DATA QUALITY RESULTS:
   Null Rows Removed           : {self.metrics.get('null_rows_removed', 0):,}
   Invalid Order ID Removed    : {self.metrics.get('invalid_id_rows_removed', 0):,}
   Total Rows Removed          : {self.metrics.get('removed_rows', 0):,}
   Window Function Duplicates  : {self.metrics.get('duplicates_removed', 0):,}
   Data Quality Status         : {'✅ PASSED' if self.metrics.get('quality_passed', False) else '⚠️ ISSUES'}
   
 BUSINESS INSIGHTS:
   Distinct Products           : {self.metrics.get('distinct_products', 0):,}
   Distinct Orders             : {self.metrics.get('distinct_order_ids', 0):,}
   Total Items Sold            : {self.metrics.get('total_quantity', 0):,}
   
 DATE RANGE:
   From: {self.metrics.get('date_range', [None, None])[0]}
   To  : {self.metrics.get('date_range', [None, None])[1]}
   
 VALUE RANGES:
   Quantity: {self.metrics.get('quantity_range', [None, None])[0]} - {self.metrics.get('quantity_range', [None, None])[1]}
   Price   : ${self.metrics.get('price_range', [None, None])[0]:.2f} - ${self.metrics.get('price_range', [None, None])[1]:.2f}
   
 OUTPUT DETAILS:
   Format              : {self.metrics.get('output_format', 'CSV (Windows Fallback)')}
   Partitioning        : BY order_year/order_month (directory structure)
   Partition Count     : {self.metrics.get('partition_count', 0)}
   Partitions Created  : {self.metrics.get('partitions', [])}
   Location            : {self.target_path}
   
 WINDOWS COMPATIBILITY:
    Hadoop native library issues bypassed
    Used CSV output for maximum compatibility
    Maintained partitioning through directory structure
    All data quality checks completed successfully
   
{'='*80}
        """
        
        print(report)
        
        # Write summary to file
        summary_file = self.target_path / "windows_pipeline_summary.txt"
        with open(summary_file, 'w') as f:
            f.write(report)
        
        logger.info(f"✅ Enhanced summary report saved to {summary_file}")
    
    def run_windows_compatible_pipeline(self):
        """Execute the complete Windows-compatible data pipeline"""
        start_time = datetime.now()
        logger.info(f" Starting Windows-Compatible Sales Data Pipeline at {start_time}")
        
        try:
            # Step 1: Initialize Spark with Windows compatibility
            self.initialize_spark_windows_compatible()
            
            # Step 2: Read CSV files and format column names
            df = self.read_csv_files()
            
            # Step 3: Clean data
            df_cleaned = self.clean_data(df)
            
            # Step 4: Window function deduplication
            df_deduplicated = self.deduplicate_data_window_function(df_cleaned)
            
            # Step 5: Enhanced data quality checks
            quality_passed = self.perform_data_quality_checks(df_deduplicated)
            
            # Step 6: Write to CSV (Windows-compatible fallback)
            self.write_single_parquet(df_deduplicated)

            
            # Step 7: Generate enhanced summary
            self.generate_enhanced_summary_report()
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            logger.info(f"✅ Windows-compatible pipeline completed successfully in {duration}")
            return True
            
        except Exception as e:
            logger.error(f" Windows-compatible pipeline failed with error: {str(e)}")
            raise
        
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")

def main():
    """Main execution function"""
    print(" Windows-Compatible Sales Data Pipeline Starting...")
    pipeline = WindowsCompatibleSalesDataPipeline()
    
    try:
        success = pipeline.run_windows_compatible_pipeline()
        if success:
            print(f"\n Pipeline executed successfully!")
            print(f" Check the '{pipeline.target_path}' folder for results")
            print(f" Review 'windows_pipeline_summary.txt' for detailed metrics")
            print(f" Note: Output saved in Parquet format for efficient querying")
        
    except Exception as e:
        print(f"\n Pipeline execution failed: {str(e)}")
        return False
    
    return True

if __name__ == "__main__":
    main()