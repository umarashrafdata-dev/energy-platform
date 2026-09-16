import os
import sys

import pytest
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

@pytest.fixture(scope="session")
def spark():
    """
    Fixture to create a SparkSession for testing.

    Returns:
        SparkSession: A SparkSession object.
    """
    spark = SparkSession.builder \
        .appName("energy-platform-tests") \
        .master("local[2]") \
        .getOrCreate()
    return spark