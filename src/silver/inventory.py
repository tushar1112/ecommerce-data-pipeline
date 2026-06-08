from pyspark.sql.functions import *
from pyspark.sql import Window

def transform_inventory(df):
    column_type_mapping = {
        "inventory_id": "string",
        "product_id": "string",
        "warehouse_id": "string",
        "available_quantity": "int",
        "reserved_quantity": "int",
        "reorder_level": "int",
        "inventory_status": "string",
        "last_restock_date": "date",
        "created_at": "timestamp",
        "updated_at": "timestamp",
        "deleted_flag": "boolean",
        "source_system": "string",
        "record_version": "int",
        # Bronze metadata
        "source_table": "string",
        "ingestion_timestamp": "timestamp",
        "ingestion_date": "date",
        "batch_id": "string"
    }

    for column_name , data_type in column_type_mapping.items():
        df = df.withColumn(column_name,col(column_name).cast(data_type))

    # drop_reason column
    df = df.withColumn("drop_reason", lit(None))

    # fixing typo and null inventory_ids
    df = df.withColumn(
        'drop_reason',
        when(col('inventory_id').isNull(),lit('Null inventory_id'))
        .when((~col('inventory_id').startswith('INV')) | (length(col('inventory_id'))!=10),lit('Invalid inventory_id'))
        .otherwise(col('drop_reason'))
    )

    inv_window = Window.partitionBy('inventory_id').orderBy(col('updated_at').desc())

    df = df.withColumn(
        'inventory_rn',
        row_number().over(inv_window)
    ).withColumn(
        'drop_reason',
        when(col('inventory_rn') > 1, lit('duplicate_inventory_rn'))
        .otherwise(col('drop_reason'))
    )

    # fixing null product id and warehouse id
    df = df.withColumn(
        'drop_reason',
        when(col('product_id').isNull(),lit('Null product_id'))
        .when(col('warehouse_id').isNull(),lit('Null warehouse_id'))
        .otherwise(col('drop_reason'))
    )

    # fixing invalid quantities and level orders
    df = df.withColumn(
        'drop_reason',
        when(col('available_quantity') < 0, lit('invalid available_quantity'))
        .when(col('reserved_quantity') < 0, lit('invalid reserved_quantity'))
        .when(col('reorder_level') < 0, lit('invalid reorder_level'))
        .otherwise(col('drop_reason'))
    )

    # last_restock_date in future fix

    df = df.withColumn(
        'drop_reason',
        when(col('last_restock_date') > current_date(), lit('Future last_restock_date'))
        .otherwise(col('drop_reason'))
    )

    df = df.withColumn("silver_processed_timestamp", current_timestamp())

    bad_records = (
        df.filter(col("drop_reason").isNotNull())
        .withColumnRenamed("drop_reason", "rejection_reason")
        .withColumn("quarantine_timestamp", current_timestamp())
        .drop("inventory_rn")
    )

    clean_df = (
        df.filter(col("drop_reason").isNull())
        .drop("inventory_rn","drop_reason")
    )

    return clean_df, bad_records