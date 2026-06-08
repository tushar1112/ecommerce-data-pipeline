from pyspark.sql import Window
from pyspark.sql.functions import *

def transform_orders(df):
    column_type_mapping = {
        "order_id": "string",
        "customer_id": "string",
        "order_date": "timestamp",
        "expected_delivery_date": "timestamp",
        "delivered_date": "timestamp",
        "total_amount": "double",
        "total_discount_amount": "double",
        "final_payable_amount": "double",
        "order_status": "string",
        "payment_status": "string",
        "shipping_city": "string",
        "shipping_state": "string",
        "shipping_postal_code": "string",
        "order_source": "string",
        "device_type": "string",
        "created_at": "timestamp",
        "updated_at": "timestamp",
        "deleted_flag": "boolean",
        "source_system": "string",
        "record_version": "int",
        # Bronze metadata
        "source_table": "string",
        "ingestion_timestamp": "timestamp",
        "ingestion_date": "date",
        "batch_id": "string",
        "operation_type": "string"
    }
    for column_name , data_type in column_type_mapping.items():
        df = df.withColumn(column_name,col(column_name).cast(data_type))

    # drop_reason column
    df = df.withColumn("drop_reason", lit(None).cast('string'))


    #1. fix invalid order_id,customer_id
    df = df.withColumn(
        'drop_reason',
        when(col('order_id').isNull(),'Blank order id')\
        .when(~col('order_id').startswith('ORD') | (length(col('order_id'))!=11),'Invalid order id') \
        .when(col("customer_id").isNull() , lit('Blank customer_id'))
        .when(~col("customer_id").startswith("CUST") |
                 (length(col("customer_id")) != 10), 'Invalid customer ID')
        .otherwise(col('drop_reason'))
    )

    # fixing duplicate order id
    order_window = Window.partitionBy('order_id').orderBy(col('updated_at').desc())
    df = df.withColumn(
        'order_rn',
        row_number().over(order_window)
    ).withColumn(
        'drop_reason',
        when(col('order_rn') > 1 , lit('duplicate order_id'))
        .otherwise(col('drop_reason'))
    )

    # fixing negative total amount and total discount

    df = df.withColumn(
        'drop_reason',
        when(col('total_amount') < 0 , lit('negative total amount'))
        .when(col('total_discount_amount') < 0 , lit('negative total discount'))
        .when(col('final_payable_amount') < 0, lit('negative payable amount'))
        .otherwise(col('drop_reason'))
    )

    # handling dates
    df = df.withColumn(
        'drop_reason',
        when(col('order_date') > col('delivered_date') , lit('delivery before order'))
        .when(col('order_date') > col('expected_delivery_date') , lit('expected delivery before order'))
        .otherwise(col('drop_reason'))
    )

    order_status_list = ["PLACED",
            "CONFIRMED",
            "SHIPPED",
            "DELIVERED",
            "CANCELLED",
            "RETURNED"]

    df = df.withColumn(
        'order_status',
        when(col('order_status').isin(order_status_list) , col('order_status'))
        .otherwise(lit('Unknown'))
    )

    payment_status = ["PENDING",
            "SUCCESS",
            "FAILED",
            "REFUNDED"]

    df = df.withColumn(
        'payment_status',
        when(col('payment_status').isin(payment_status), col('payment_status'))
        .otherwise(lit('Unknown'))
    )

    df = df.withColumn("silver_processed_timestamp", current_timestamp())

    bad_records = (
        df.filter(col("drop_reason").isNotNull())
        .withColumnRenamed("drop_reason", "rejection_reason")
        .withColumn("quarantine_timestamp", current_timestamp())
        .drop("order_rn")
    )

    clean_df = (
        df.filter(col("drop_reason").isNull())
        .drop("order_rn","drop_reason")
    )

    return clean_df, bad_records