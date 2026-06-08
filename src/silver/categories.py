from pyspark.sql.functions import *
from pyspark.sql import Window

def transform_categories(df):
    column_type_mapping = {
        "category_id": "string",
        "category_name": "string",
        "parent_category_id": "string",
        "hierarchy_level": "int",
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

    #1 category id null/invalid and its duplicates
    df = df.withColumn(
        'drop_reason',
        when(col('category_id').isNull() | (~col('category_id').startswith('CAT')),lit('Invalid category id'))
        .otherwise(col('drop_reason'))
    )
    cat_window = Window.partitionBy('category_id').orderBy(col('updated_at').desc())
    df = df.withColumn(
        'category_rn',
        row_number().over(cat_window)
    ).withColumn(
        'drop_reason',
        when(col('category_rn') > 1,lit('duplicate category id'))
        .otherwise(col('drop_reason'))
    )

    # fixing null category name, null hierarchy level
    df = df.withColumn(
        'drop_reason',
        when(col('category_name').isNull(), lit('null category name'))
        .when(col('hierarchy_level').isNull(), lit('null hierarchy level'))
        .otherwise(col('drop_reason'))
    )

    #fixing null parent category id except level 1

    df = df.withColumn(
        'drop_reason',
        when((col('parent_category_id').isNull()) & (col('hierarchy_level') > 1), lit('Null parent id'))
        .otherwise(col('drop_reason'))
    )

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
        .drop("category_rn")
    )

    clean_df = (
        df.filter(col("drop_reason").isNull())
        .drop("category_rn", "drop_reason")
    )

    return clean_df, bad_records



