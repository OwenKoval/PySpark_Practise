"""
Module with all needed for tests utils
"""

from pyspark.sql import Row


def df_to_list_of_dict(df):
    """
    Convert input df to the list of the dictionaries
    :param df: spark dataframe of list of the Row`s objects
    :return: list of the dicts.
    """
    return list(map(Row.asDict, isinstance(df, list) and df or df.collect()))


def assert_df_equal(df1, df2):
    """
    Assert that df1 equal to df2
    :param df1: spark dataframe of list of the Row`s objects
    :param df2: spark dataframe of list of the Row`s objects
    """
    df1_content = df_to_list_of_dict(df1)
    df2_content = df_to_list_of_dict(df2)
    assert len(df1_content) == len(df2_content)
    for df1_item in df1_content:
        assert df1_item in df2_content


def reader_test_csv(spark, path=None, schema_csv=None,
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

    return (spark.read
            .options(header=header, sep=sep)
            .csv(path, inferSchema=inferschema, schema=schema_csv))


def reader_test_json(spark, path=None, schema_json=None,
                     header='false', sep=','):
    """
    :param spark: SparkSession to read the file
    :param path: path to file to read it
    :param schema_json: use schema which defined by developer
    :param sep: while reading use sep to separate the data
    :param header: true or false use header in reading df
    :return csv_df: dataframe with data
    """

    return (spark.read
            .options(header=header, sep=sep)
            .json(path, schema=schema_json))
