#!/usr/bin/env python3
"""
Environment Verification Script for PySpark Data Pipeline
This script verifies that all required components are properly installed and configured.
"""

import sys
import os
from pathlib import Path

def check_python_version():
    """Check Python version compatibility"""
    print("=" * 50)
    print("1. PYTHON VERSION CHECK")
    print("=" * 50)
    
    python_version = sys.version_info
    print(f"Python Version: {python_version.major}.{python_version.minor}.{python_version.micro}")
    
    if python_version.major == 3 and python_version.minor >= 7:
        print("✅ Python version is compatible")
        return True
    else:
        print("❌ Python version should be 3.7 or higher")
        return False

def check_pyspark_installation():
    """Check if PySpark is installed and working"""
    print("\n" + "=" * 50)
    print("2. PYSPARK INSTALLATION CHECK")
    print("=" * 50)
    
    try:
        import pyspark
        print(f"✅ PySpark installed - Version: {pyspark.__version__}")
        
        # Try to create a SparkSession
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("EnvironmentTest") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        print("✅ SparkSession created successfully")
        
        # Test basic DataFrame operations
        test_data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        columns = ["Name", "Age"]
        df = spark.createDataFrame(test_data, columns)
        
        row_count = df.count()
        print(f"✅ Basic DataFrame operations working - Test DF has {row_count} rows")
        
        spark.stop()
        return True
        
    except ImportError:
        print("❌ PySpark not installed. Install with: pip install pyspark")
        return False
    except Exception as e:
        print(f"❌ Error with PySpark: {str(e)}")
        return False

def check_pandas_installation():
    """Check if Pandas is installed (useful for data exploration)"""
    print("\n" + "=" * 50)
    print("3. PANDAS INSTALLATION CHECK")
    print("=" * 50)
    
    try:
        import pandas as pd
        print(f"✅ Pandas installed - Version: {pd.__version__}")
        
        # Test basic pandas operation
        test_df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
        print(f"✅ Basic Pandas operations working - Test DF shape: {test_df.shape}")
        return True
        
    except ImportError:
        print("❌ Pandas not installed. Install with: pip install pandas")
        return False
    except Exception as e:
        print(f"❌ Error with Pandas: {str(e)}")
        return False

def check_java_installation():
    """Check Java installation (required for Spark)"""
    print("\n" + "=" * 50)
    print("4. JAVA INSTALLATION CHECK")
    print("=" * 50)
    
    java_home = os.environ.get('JAVA_HOME')
    if java_home:
        print(f"✅ JAVA_HOME set to: {java_home}")
    else:
        print("⚠️  JAVA_HOME not set (PySpark will try to find Java automatically)")
    
    # Try to run java -version
    try:
        import subprocess
        result = subprocess.run(['java', '-version'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            java_version = result.stderr.split('\n')[0] if result.stderr else result.stdout.split('\n')[0]
            print(f"✅ Java is accessible: {java_version}")
            return True
        else:
            print("❌ Java command failed")
            return False
    except FileNotFoundError:
        print("❌ Java not found in PATH")
        print("   Install Java 8 or 11, or set JAVA_HOME")
        return False
    except Exception as e:
        print(f"❌ Error checking Java: {str(e)}")
        return False

def check_folder_structure():
    """Check and create required folders"""
    print("\n" + "=" * 50)
    print("5. FOLDER STRUCTURE CHECK")
    print("=" * 50)
    
    required_folders = ['Sales_Data', 'cleansed']
    current_dir = Path.cwd()
    
    print(f"Current working directory: {current_dir}")
    
    for folder in required_folders:
        folder_path = current_dir / folder
        if folder_path.exists():
            file_count = len(list(folder_path.glob('*')))
            print(f"✅ {folder}/ exists - Contains {file_count} items")
        else:
            try:
                folder_path.mkdir(parents=True, exist_ok=True)
                print(f"✅ {folder}/ created successfully")
            except Exception as e:
                print(f"❌ Failed to create {folder}/: {str(e)}")
                return False
    
    return True

def check_csv_files():
    """Check if CSV files exist in Sales_Data folder"""
    print("\n" + "=" * 50)
    print("6. DATA FILES CHECK")
    print("=" * 50)
    
    sales_data_path = Path.cwd() / 'Sales_Data'
    csv_files = list(sales_data_path.glob('*.csv'))
    
    if csv_files:
        print(f"✅ Found {len(csv_files)} CSV files in Sales_Data/:")
        for i, file in enumerate(csv_files[:5], 1):  # Show first 5 files
            file_size = file.stat().st_size / 1024  # Size in KB
            print(f"   {i}. {file.name} ({file_size:.1f} KB)")
        if len(csv_files) > 5:
            print(f"   ... and {len(csv_files) - 5} more files")
        return True
    else:
        print("❌ No CSV files found in Sales_Data/ folder")
        print("   Make sure your data files are in the Sales_Data/ directory")
        return False

def run_comprehensive_verification():
    """Run all verification checks"""
    print("PYSPARK ENVIRONMENT VERIFICATION")
    print("=" * 50)
    print("This script will verify your environment setup for the data pipeline project.\n")
    
    checks = [
        ("Python Version", check_python_version),
        ("PySpark Installation", check_pyspark_installation),
        ("Pandas Installation", check_pandas_installation),
        ("Java Installation", check_java_installation),
        ("Folder Structure", check_folder_structure),
        ("Data Files", check_csv_files)
    ]
    
    results = {}
    for check_name, check_function in checks:
        try:
            results[check_name] = check_function()
        except Exception as e:
            print(f"❌ Unexpected error in {check_name}: {str(e)}")
            results[check_name] = False
    
    # Summary
    print("\n" + "=" * 50)
    print("VERIFICATION SUMMARY")
    print("=" * 50)
    
    passed = sum(results.values())
    total = len(results)
    
    for check_name, passed_check in results.items():
        status = "✅ PASS" if passed_check else "❌ FAIL"
        print(f"{check_name:<20}: {status}")
    
    print(f"\nOverall: {passed}/{total} checks passed")
    
    if passed == total:
        print("\n🎉 All checks passed! Your environment is ready for the data pipeline project.")
    else:
        print("\n⚠️  Some checks failed. Please address the issues above before proceeding.")
        print("\nQuick fixes:")
        if not results.get("PySpark Installation", True):
            print("- Install PySpark: pip install pyspark")
        if not results.get("Pandas Installation", True):
            print("- Install Pandas: pip install pandas")
        if not results.get("Java Installation", True):
            print("- Install Java 8 or 11 and ensure it's in your PATH")
        if not results.get("Data Files", True):
            print("- Place your CSV files in the Sales_Data/ folder")
    
    return passed == total

if __name__ == "__main__":
    success = run_comprehensive_verification()
    sys.exit(0 if success else 1)