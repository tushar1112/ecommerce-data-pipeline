from pyspark.sql.functions import *
from pyspark.sql import Window

def transform_suppliers(df):
    column_type_mapping = {
        "supplier_id": "string",
        "supplier_name": "string",
        "supplier_type": "string",

        "contact_person": "string",
        "email": "string",
        "phone": "string",

        "city": "string",
        "state": "string",
        "country": "string",
        "postal_code": "string",

        "supplier_status": "string",
        "contract_start_date": "date",
        "contract_end_date": "date",

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

    # validity supplier id and null, then duplicates
    df = df.withColumn(
        'drop_reason',
        when(col('supplier_id').isNull() | (~col('supplier_id').startswith('SUP')),lit('Invalid supplier id'))
        .when(col('supplier_name').isNull(), lit('blank supplier name'))
        .otherwise(col('drop_reason'))
    )

    supply_window = Window.partitionBy('supplier_id').orderBy(col('updated_at').desc())
    df = df.withColumn(
        'supply_rn',
        row_number().over(supply_window)
    ).withColumn(
        'drop_reason',
        when(col('supply_rn') > 1, lit('duplicate supplier id'))
        .otherwise(col('drop_reason'))
    )

    #fixing supplier type
    valid_supplier_type = [
        "Distributor",
        "Wholesaler",
        "Retail Vendor",
        "Marketplace Seller",
        "Internal Warehouse Supplier"
    ]
    df = df.withColumn(
        'supplier_type',
        when(~col('supplier_type').isin(valid_supplier_type), lit('Unknown'))
        .otherwise(col('supplier_type'))
    )

    df = df.withColumn(
        'Invalid_supplier_type',
        when(col('supplier_type')=="Unknown", lit(True))
        .otherwise(lit(False))
    )

    # flagging records with invalid phone or email
    email_regex = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    india_mobile_regex = r"^(91)?[6-9]\d{9}$"

    df = df.withColumn(
        'invalid_email_flag',
        when(~col('email').rlike(email_regex), lit(True))
        .otherwise(lit(False))
    )
    df = df.withColumn(
        'invalid_phone_flag',
        when(~col('phone').rlike(india_mobile_regex), lit(True))
        .otherwise(lit(False))
    )

    df = df.withColumn(
      'email',when(col('email').isNull(), lit('unknown')).otherwise(col('email'))
    )

    df = df.withColumn(
        'phone', when(col('phone').isNull(), lit('unknown')).otherwise(col('phone'))
    )

    # fixing invalid contract start and end date
    df = df.withColumn(
        'drop_reason',
        when(col('contract_end_date') < col('contract_start_date') , lit('contract end before start date'))
        .otherwise(col('drop_reason'))
    )

    # city and state nulls
    df = df.withColumn(
        'null_city_state_flag',
        when(col('city').isNull() | col('state').isNull() , lit(True))
        .otherwise(lit(False))
    )

    # 8 data quality status
    df = df.withColumn(
        "data_quality_status",
        when(
            (col("null_city_state_flag") == True)
            | (col("invalid_phone_flag") == True)
            | (col("invalid_email_flag") == True)
            | (col("Invalid_supplier_type") == True),
            lit("WARNING")
        ).otherwise(lit("VALID"))
    )

    df = df.withColumn("silver_processed_timestamp", current_timestamp())

    bad_records = (
        df.filter(col("drop_reason").isNotNull())
        .withColumnRenamed("drop_reason", "rejection_reason")
        .withColumn("quarantine_timestamp", current_timestamp())
        .drop("supply_rn")
    )

    clean_df = (
        df.filter(col("drop_reason").isNull())
        .drop("drop_reason", "supply_rn")
    )

    return clean_df, bad_records