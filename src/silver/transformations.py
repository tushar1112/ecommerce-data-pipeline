from wsgiref import validate
import builtins
import time
from pyspark.sql import functions as F
from pyspark.sql.functions import *

from src.common.watermark import get_last_processed_batch, update_watermark
from src.silver.categories import transform_categories
from src.silver.common.referential_integrity import validate_product_with_category_supplier, \
    validate_inventory_with_product_warehouse, validate_orders_with_customers, \
    validate_orderitems_with_product_order_warehouse, validate_payments_with_order
from src.silver.customers import transform_customers
from src.silver.inventory import transform_inventory
from src.silver.orders import transform_orders
from src.silver.order_items import transform_order_items
from src.silver.products import transform_products
from src.silver.payments import transform_payments
from src.silver.suppliers import transform_suppliers
from src.silver.warehouses import transform_warehouses


def read_bronze_data(spark, file_path):
    return spark.read.parquet(file_path)

def trim_string_columns(df):
    column_list = list(df.columns)
    for col_name in column_list:
        df = df.withColumn(col_name, F.trim(col_name))
    return df

def common_cleaning(df):
    df = trim_string_columns(df)
    return df

def apply_dataset_transformations(df, dataset_name):
    if dataset_name == "customers":
        return transform_customers(df)
    elif dataset_name == "inventory":
        return transform_inventory(df)
    elif dataset_name == "products":
        return transform_products(df)
    elif dataset_name == "orders":
        return transform_orders(df)
    elif dataset_name == "order_items":
        return transform_order_items(df)
    elif dataset_name == "payments":
        return transform_payments(df)
    elif dataset_name == "categories":
        return transform_categories(df)
    elif dataset_name == "suppliers":
        return transform_suppliers(df)
    elif dataset_name == "warehouses":
        return transform_warehouses(df)
    else:
        empty_reject = df.limit(0).withColumn(
            "drop_reason",
            F.lit(None)
        )
        return df, empty_reject

def write_silver(spark,df, output_path, temp_path):
    df.write.mode('overwrite').parquet(temp_path)
    new_df = spark.read.parquet(temp_path)
    new_df.write.mode('overwrite').parquet(output_path)


def write_quarantine(df, output_path):
    df.write.mode('append').parquet(output_path)

def reference_checks(logger,clean,rejected):
    # going for referential integrity check
    logger.info('***    Starting referential integrity checks and final cleaning    ***')
    print('***    Starting referential integrity checks and final cleaning    ***')

    # 1. products -> categories and suppliers
    clean['products'], rejected_products = validate_product_with_category_supplier(
                clean['products'],clean['suppliers'],clean['categories']
    )
    rejected['products'] = rejected['products'].unionByName(rejected_products,allowMissingColumns=True)

    # 2. inventory -> products and warehouses
    clean['inventory'], rejected_inventory = validate_inventory_with_product_warehouse(
                clean['inventory'],clean['warehouses'],clean['products']
    )
    rejected['inventory'] = rejected['inventory'].unionByName(rejected_inventory,allowMissingColumns=True)

    # 3. orders -> customer
    clean['orders'], rejected_orders = validate_orders_with_customers(
        clean['orders'], clean['customers']
    )
    rejected['orders'] = rejected['orders'].unionByName(rejected_orders,allowMissingColumns=True)

    # 4. order_items -> orders, products , warehouses
    clean['order_items'], rejected_order_items = validate_orderitems_with_product_order_warehouse(
        clean['order_items'], clean['warehouses'], clean['products'], clean['orders']
    )
    rejected['order_items'] = rejected['order_items'].unionByName(rejected_order_items,allowMissingColumns=True)

    # 4. payments -> orders, customers
    clean['payments'], rejected_payments = validate_payments_with_order(
        clean['payments'], clean['orders']
    )
    rejected['payments'] = rejected['payments'].unionByName(rejected_payments,allowMissingColumns=True)

    logger.info('***   referential integrity checks completed     ***')
    print('***   referential integrity checks completed     ***')


def silver_full_processing(spark,logger,config):
    bronze_path = config["paths"]["bronze"]
    silver_path = config["paths"]["silver"]
    audit_path  = config["paths"]["audit"]
    temp_path = config["paths"]["temp"]
    reject_path_silver = config["paths"]["rejected_data_silver"]
    files = config["files"]
    screenshot_path = config["paths"]["screenshot"]
    raw = {}
    clean = {}
    rejected = {}

    for file_name in files:
        dataset_name = file_name.replace('.csv', '')
        read_path_ = f'{bronze_path}/{dataset_name}'

        df = read_bronze_data(spark, read_path_)
        raw[dataset_name] = df

        df = common_cleaning(df)

        good_df, reject_df = apply_dataset_transformations(df, dataset_name)

        clean[dataset_name] = good_df
        rejected[dataset_name] = reject_df
        logger.info(f'Initial silver cleaning for {dataset_name} completed')

        # checking referential integrity
    reference_checks(logger, clean, rejected)

    summary_row = []
    columns = [
        "table_name",
        "batch_id",
        "bronze_count",
        "silver_count",
        "quarantine_count",
        "rejected_percentage",
        "status",
        "load_type"
    ]

    for file_name in files:
        dataset_name = file_name.replace('.csv', '')
        bronze_count = raw[dataset_name].count()
        clean[dataset_name] = clean[dataset_name].cache()
        rejected[dataset_name] = rejected[dataset_name].cache()
        silver_count = clean[dataset_name].count()
        quarantine_count = rejected[dataset_name].count()
        batch_id = clean[dataset_name].first()['batch_id']
        logger.info(f' Writing files into silver and quarantine path for {dataset_name}')
        write_silver(spark,clean[dataset_name], f'{silver_path}/{dataset_name}',f'{temp_path}/{dataset_name}')
        write_quarantine(rejected[dataset_name], f'{reject_path_silver}/{dataset_name}')
        logger.info(f' Writing done for {dataset_name}')

        rejected_percentage = (
            builtins.round((quarantine_count / bronze_count) * 100, 2)
            if bronze_count > 0 else 0.0
        )

        status = "Failed" if rejected_percentage > 5 else "Success"
        summary_row.append((
            dataset_name,
            batch_id,
            bronze_count,
            silver_count,
            quarantine_count,
            rejected_percentage,
            status,
            "FULL_LOAD"
        ))

    dq_summary_df = spark.createDataFrame(summary_row, columns)
    dq_summary_df.show()
    time.sleep(5)
    dq_summary_df.coalesce(1).write.mode('overwrite').parquet(f'{audit_path}/dq_summary')

def silver_incremental_processing(spark,logger,config):

    bronze_path = config["paths"]["bronze"]
    silver_path = config["paths"]["silver"]
    temp_path = config["paths"]["temp"]
    audit_path = config["paths"]["audit"]
    reject_path_silver = config["paths"]["rejected_data_silver"]
    files_inc = config["files_inc"]
    watermark_path = config["paths"]["watermark"]
    screenshot_path = config["paths"]["screenshot"]

    summary_row = []
    columnss = [
        "table_name",
        "batch_id",
        "bronze_count",
        "silver_count",
        "quarantine_count",
        "rejected_percentage",
        "status",
        "load_type"
    ]

    for dataset_name, metadata in files_inc.items():
        # latest batch_id using watermark
        latest_batch_id = get_last_processed_batch(
            spark=spark,
            watermark_path=watermark_path,
            table_name=dataset_name,
            layer="bronze"
        )
        print(latest_batch_id)
        try:
            read_path_ = f'{bronze_path}/{dataset_name}'
            bronze_df = read_bronze_data(spark, read_path_)
            joining_key = metadata["joining_key"]


            incremental_df = bronze_df.filter(
                col('batch_id') == latest_batch_id
            )
            bronze_count = incremental_df.count()

            incremental_df = common_cleaning(incremental_df)

            print(f'incremental_df for {dataset_name}')
            incremental_df.show(5,truncate=False)
            print(f'incremental_df count {incremental_df.count()}')

            good_df, reject_df = apply_dataset_transformations(incremental_df, dataset_name)
            quarantine_count = reject_df.count()

            print(f' good_df after transformations count: {good_df.count()}')
            print(f' reject_df after transformations count: {reject_df.count()}')

            current_silver_df = spark.read.parquet(f'{silver_path}/{dataset_name}/')

            changed_keys = good_df.select(joining_key).distinct()

            unchanged_silver_df = current_silver_df.join(
                changed_keys,
                on=joining_key,
                how='left_anti'
            )

            final_silver_df = unchanged_silver_df.unionByName(good_df, allowMissingColumns=True)
            silver_count = final_silver_df.count()

            write_silver(spark,final_silver_df, f'{silver_path}/{dataset_name}',f'{temp_path}/{dataset_name}')
            write_quarantine(reject_df, f'{reject_path_silver}/{dataset_name}')

            update_watermark(spark, watermark_path, dataset_name, "silver", latest_batch_id, "SUCCESS")

            # adding audit data
            insert_count = good_df.filter(
                col("operation_type") == "INSERT"
            ).count()

            update_count = good_df.filter(
                col("operation_type") == "UPDATE"
            ).count()

            delete_count = good_df.filter(
                col("operation_type") == "DELETE"
            ).count()

            audit_data = [(
                latest_batch_id,
                dataset_name,
                insert_count,
                update_count,
                delete_count
            )]

            columns = [
                "batch_id",
                "table_name",
                "insert_count",
                "update_count",
                "delete_count"
            ]

            audit_summary_df = spark.createDataFrame(audit_data, columns)
            audit_summary_df.write.mode('append').parquet(f'{audit_path}/CDC_metadata')
            audit_summary_df.coalesce(1).write.mode('append').option("header", True).csv(
                f'{screenshot_path}/CDC_metadata')

            #DQ summary of increment
            rejected_percentage = (
                builtins.round((quarantine_count / bronze_count) * 100, 2)
                if bronze_count > 0 else 0.0
            )
            status = "Failed" if rejected_percentage > 8 else "Success"

            summary_row.append((
                dataset_name,
                latest_batch_id,
                bronze_count,
                silver_count,
                quarantine_count,
                rejected_percentage,
                status,
                "INCREMENTAL"
            ))

        except Exception as e:
            update_watermark(spark, watermark_path, dataset_name, "silver", latest_batch_id, "FAILED")
            raise

    dq_summary_df = spark.createDataFrame(summary_row, columnss)
    dq_summary_df.show()
    dq_summary_df.write.mode('append').parquet(f'{audit_path}/dq_summary')
    dq_summary_df.coalesce(1).write.mode('overwrite').option("header", True).csv(f'{screenshot_path}/dq_summary')










