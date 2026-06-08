from pyspark.sql import Window
from pyspark.sql.functions import *

def transform_order_items(df):

    column_type_mapping = {
        "order_item_id": "string",
        "order_id": "string",
        "product_id": "string",
        "warehouse_id": "string",
        "quantity": "int",
        "unit_price": "double",
        "gross_amount": "double",
        "discount_amount": "double",
        "final_price": "double",
        "item_status": "string",
        "estimated_shipping_cost": "double",
        "created_at": "timestamp",
        "updated_at": "timestamp",
        "deleted_flag": "boolean",
        "source_system": "string",
        "record_version": "int",
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

    #filtering null values at order_item_id
    df = df.withColumn(
        'drop_reason',
        when(col('order_item_id').isNull() | (~col('order_item_id').startswith('OI')),lit('Invalid order item id'))
        .otherwise(col('drop_reason'))
        )
    # fixing duplicate order_item_id
    order_window = Window.partitionBy('order_item_id').orderBy(col('updated_at').desc_nulls_last(),
                                                               col('ingestion_timestamp').desc_nulls_last())
    df = df.withColumn(
        'order_rn',
        row_number().over(order_window)
    ).withColumn(
        'drop_reason',
        when(col('order_rn') > 1, lit('duplicate order_item_id'))
        .otherwise(col('drop_reason'))
    )

    #  order id, product id, warehouse id
    df = df.withColumn(
        'drop_reason',
        when(col('order_id').isNull() , lit('Null order id'))
        .when(col('product_id').isNull() , lit('Null product id'))
        .when(col('warehouse_id').isNull() , lit('Null warehouse id'))
        .otherwise(col('drop_reason'))
    )

    # numerical columns validations quantity	unit_price	gross_amount	discount_amount	final_price
    df = df.withColumn(
        'drop_reason',
        when(col('quantity').isNull() | (col('quantity')< 0), lit('Invalid quantity'))
        .when(col('unit_price').isNull() | (col('unit_price')< 0), lit('Invalid unit_price'))
        .when(col('gross_amount').isNull() | (col('gross_amount') < 0), lit('Invalid gross_amount'))
        .when(col('discount_amount').isNull() | (col('discount_amount') < 0), lit('Invalid discount_amount'))
        .when(col('final_price').isNull() | (col('final_price') < 0), lit('Invalid final_price'))
        .otherwise(col('drop_reason'))
    )

    # negative shipping cost flag
    df = df.withColumn(
        'invalid_shipping_cost_flag',
        when(col('estimated_shipping_cost') < 0 , lit(True))
        .otherwise(lit(False))
    )

    valid_status = [
        "PROCESSING",
        "SHIPPED",
        "DELIVERED",
        "CANCELLED",
        "RETURNED"
    ]

    df= df.withColumn(
        'invalid_item_status_flag',
        when(~col('item_status').isin(valid_status),lit(True))
        .otherwise(lit(False))
    )
    df = df.withColumn(
        "item_status",
        when(col("item_status").isin(valid_status), col("item_status"))
        .otherwise(lit("UNKNOWN"))
    )

    # data quality
    df = df.withColumn(
        "data_quality_status",
        when(
            (col("invalid_shipping_cost_flag") == True)
            | (col("invalid_item_status_flag") == True),
            lit("WARNING")
        ).otherwise(lit("VALID"))
    )

    df = df.withColumn(
        'silver_processed_timestamp', current_timestamp()
    )

    bad_records = (
        df.filter(col("drop_reason").isNotNull())
        .withColumnRenamed("drop_reason", "rejection_reason")
        .withColumn("quarantine_timestamp", current_timestamp())
        .drop("order_rn")
    )

    clean_df = (
        df.filter(col("drop_reason").isNull())
        .drop("order_rn", "drop_reason")
    )

    return clean_df, bad_records






