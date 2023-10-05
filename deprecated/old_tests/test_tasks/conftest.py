import pytest

from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder \
        .master('local') \
        .appName('Test') \
        .config('spark.sql.legacy.timeParserPolicy', 'LEGACY') \
        .config('spark.shuffle.partitions', 4) \
        .getOrCreate()
