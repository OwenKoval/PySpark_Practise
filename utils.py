"""
Utils file with main functions

"""

from pyspark.sql import SparkSession


def spark_builder(appname='Task 2', partitions=4):
    """
    The entry point of SparkSession
    :param appname: set the name of your SparkSession
    :param partitions: set the number of partitions
    """
    return (SparkSession.builder
            .master('local')
            .appName(appname)
            .config('spark.sql.legacy.timeParserPolicy', 'LEGACY')
            .config('spark.shuffle.partitions', partitions)
            .getOrCreate())


def read_csv(spark, path=None, schema_csv=None,
             header='false', sep=',', inferschema='false'):
    """
    :param spark: SparkSession to read the file
    :param path: path to file to read it
    :param schema_csv: use schema which defined by developer
    :param sep: while reading use sep to separate the data
    :param header: true or false use header in reading df
    :param inferschema: use inferschema in reading
    :return csv_df: dataframe with data
    """
    if path is None:
        raise ValueError('Path cannot be None')

    return (spark.read
            .options(header=header, sep=sep)
            .csv(path, inferSchema=inferschema, schema=schema_csv))


def read_json(spark, path=None, schema_json=None,
              header='false', sep=',', multiline=True):
    """
    :param spark: SparkSession to read the file
    :param path: path to file to read it
    :param schema_json: use schema which defined by developer
    :param sep: while reading use sep to separate the data
    :param header: true or false use header in reading df
    :param multiline: true or false use multiline in reading df
    :return csv_df: dataframe with data
    """
    if path is None:
        raise ValueError('Path cannot be None')

    return (spark.read
            .options(header=header, sep=sep, multiline=multiline)
            .json(path, schema=schema_json))


def write_to_csv(df, file_path='', header='true'):
    """
    Write the df to folder
    :param header: true or false use header in writing result
    :param file_path: path to folder where result will save
    :param df: result dataframe after transformation
    :return: csv file with results
    """
    (df.write
     .option('header', header)
     .mode('overwrite')
     .csv(file_path))


def write_to_json(df, file_path='', header='true'):
    """
    Write the df to folder with format json
    :param header: true or false use header in writing result
    :param file_path: path to folder where result will save
    :param df: result dataframe after transformation
    :return: json file with results
    """
    (df.write
     .option('header', header)
     .mode('overwrite')
     .json(file_path))
