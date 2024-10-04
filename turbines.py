# Databricks notebook source
from pyspark.sql.functions import col, mean, stddev, min, max, avg

def load_data(file_paths):
    df_all = [spark.read.option("header", "true").csv(path, inferSchema=True) for path in file_paths]
    df_union = df_all[0]
    for df in df_all[1:]:
        df_union = df_union.union(df)
    return df_union

def clean_data(df):
    df = df.dropna()
    df = df.filter((col("power_output") >= 0) & (col("power_output") <= 10))
    return df

def calculate_stats(df):
    df = df.groupBy("turbine_id").agg(min("power_output").alias("min_output"),max("power_output").alias("max_output"),avg("power_output").alias("avg_output")
    )
    return df

def identify_anomalies(df):
    mean_val = df.select(mean("power_output")).collect()[0][0]
    stddev_val = df.select(stddev("power_output")).collect()[0][0]
    
    upper_limit = mean_val + 2 * stddev_val
    lower_limit = mean_val - 2 * stddev_val
    
    df = df.filter((col("power_output") > upper_limit) | (col("power_output") < lower_limit))
    return df

def store_data(df, table_name):
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)

file_paths = ["/FileStore/tables/data_group_1.csv", "/FileStore/tables/data_group_2.csv", "/FileStore/tables/data_group_3.csv"]

turbine_df = load_data(file_paths)
cleaned_df = clean_data(turbine_df)
summary_df = calculate_stats(cleaned_df)
anomalies_df = identify_anomalies(cleaned_df)


store_data(cleaned_df, "processed_turbine_data")
store_data(summary_df, "turbine_summary_statistics")
store_data(anomalies_df, "turbine_anomalies")


cleaned_df.display()
summary_df.display()
anomalies_df.display()

