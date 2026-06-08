from pyspark.sql.functions import *
from pyspark.sql import Window

def transform_warehouses(df):
    column_type_mapping = {
        "warehouse_id": "string",
        "warehouse_name": "string",
        "warehouse_type": "string",

        "city": "string",
        "state": "string",
        "postal_code": "string",

        "warehouse_capacity": "int",
        "manager_name": "string",
        "contact_number": "string",
        "is_active": "boolean",

        "created_at": "timestamp",
        "updated_at": "timestamp",
        "deleted_flag": "boolean",
        "source_system": "string",
        "record_version": "int",

        "source_table": "string",
        "ingestion_timestamp": "timestamp",
        "ingestion_date": "date",
        "batch_id": "string"
    }

    for column_name, column_type in column_type_mapping.items():
        df = df.withColumn(column_name, col(column_name).cast(column_type))

    df = df.withColumn('drop_reason', lit(None).cast('string'))

    # validity warehouse_id (null and duplicates)
    df = df.withColumn(
        'drop_reason',
        when(col('warehouse_id').isNull() | (~col('warehouse_id').startswith('WH')), lit('Invalid warehouse id'))
        .when(col('warehouse_name').isNull(), lit('blank warehouse name'))
        .otherwise(col('drop_reason'))
    )

    warehouse_window = Window.partitionBy('warehouse_id').orderBy(col('updated_at').desc())
    df = df.withColumn(
        'warehouse_rn',
        row_number().over(warehouse_window)
    ).withColumn(
        'drop_reason',
        when(col('warehouse_rn') > 1 , lit('duplicate warehouse id'))
        .otherwise(col('drop_reason'))
    )

    # warehouse type fixing
    warehouse_type = [
        "Fulfillment Center",
        "Regional Warehouse",
        "Dark Store",
        "Sortation Center"
    ]
    df = df.withColumn(
        'warehouse_type',
        when(~col('warehouse_type').isin(warehouse_type), lit('unknown'))
        .otherwise(col('warehouse_type'))
    )
    df.withColumn(
        'invalid_warehouse_type_flag',
        when(col('warehouse_type')=='unknown', lit(True))
        .otherwise(lit(False))
    )

    # city, state or postal code null

    df = df.withColumn(
        'invalid_city_state_pincode_flag',
        when(col('city').isNull() | col('state').isNull() | col('postal_code').isNull(), lit(True))
        .otherwise(lit(False))
    )

    # warehouse capacity check
    df = df.withColumn(
        'drop_reason',
        when(col('warehouse_capacity') <= 0 , lit('invalid warehouse capacity'))
        .otherwise(col('drop_reason'))
    )

    # is_active null
    df = df.withColumn(
        'is_active',
        when(col('is_active').isNull(), lit(False))
        .otherwise(col('is_active'))
    )

    df = df.withColumn("silver_processed_timestamp", current_timestamp())

    bad_records = (
        df.filter(col("drop_reason").isNotNull())
        .withColumnRenamed("drop_reason", "rejection_reason")
        .withColumn("quarantine_timestamp", current_timestamp())
        .drop("warehouse_rn")
    )

    clean_df = (
        df.filter(col("drop_reason").isNull())
        .drop("drop_reason", "warehouse_rn")
    )

    return clean_df, bad_records