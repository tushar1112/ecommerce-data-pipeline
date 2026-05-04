# empty object for unix features (error handle)
import socketserver
import os

if os.name == 'nt':
    socketserver.UnixStreamServer = object
    socketserver.UnixDatagramServer = object
#---------------------------------------------main code------------------------------------------------
from pyspark.sql import functions as F
from src.silver.customers import transform_customers
from src.silver.orders import transform_orders
from src.silver.order_details import transform_order_details
from src.silver.products import transform_products
from src.silver.payments import transform_payments
from src.silver.reviews import transform_reviews
from src.silver.sellers import transform_sellers

def read_bronze_data(spark, file_path):
    return (spark.read.parquet(file_path))

def trim_string_columns(df):
    for col_name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(col_name, F.trim(col_name))
    return df

def remove_duplicates(df):
    return df.dropDuplicates()


def add_processed_timestamp(df):
    return df.withColumn(
        "processed_timestamp",
        F.current_timestamp()
    )

def common_cleaning(df):
    df = trim_string_columns(df)
    df = remove_duplicates(df)
    df = add_processed_timestamp(df)
    return df

def apply_dataset_transformations(df, dataset_name):
    if dataset_name == "customers":
        return transform_customers(df)
    elif dataset_name == "orders":
        return transform_orders(df)
    elif dataset_name == "products":
        return transform_products(df)
    elif dataset_name == "payments":
        return transform_payments(df)
    elif dataset_name == "reviews":
        return transform_reviews(df)
    elif dataset_name == "sellers":
        return transform_sellers(df)
    elif dataset_name == "order_details":
        return transform_order_details(df)
    else:
        empty_reject = df.limit(0).withColumn(
            "Drop_reason",
            F.lit(None)
        )
        return df, empty_reject

def write_parquet(df, output_path):
    (df.write.mode('overwrite').parquet(output_path)
     )

def process_file(spark,logger,read_path,silver_path,reject_path,database_name):
    try:
        logger.info(f'Silver processing started for {database_name}')
        df = read_bronze_data(spark, read_path)
        df = common_cleaning(df)
        good_df,reject_df = apply_dataset_transformations(df, database_name)
        write_parquet(good_df,silver_path)

        # print(f'after silver processing for {database_name}, good and bad df are :')
        # good_df.show(10,truncate=False)
        # reject_df.show(10, truncate=False)

        if reject_df.count() > 0:
            write_parquet(reject_df,reject_path)
        logger.info(f'Silver processing completed for {database_name}')
        return True
    except Exception as e:
        logger.error(f'Silver processing failed for {database_name}: {str(e)}')
        return False





