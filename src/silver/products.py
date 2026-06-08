from pyspark.sql.functions import *
from pyspark.sql import Window

def transform_products(df):

    # defining schema and fixing data types
    column_type_mapping = {
        "product_id": "string",
        "product_name": "string",
        "brand": "string",
        "category_id": "string",
        "supplier_id": "string",
        "sku": "string",
        "mrp": "double",
        "selling_price": "double",
        "cost_price": "double",
        "product_weight_grams": "int",
        "product_rating": "double",
        "total_reviews": "int",
        "launch_date": "date",
        "product_status": "string",
        "is_returnable": "boolean",
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

    # Initialize rejection reason
    df = df.withColumn("drop_reason", lit(None).cast("string"))

    # fixing product id
    df = df.withColumn(
        'drop_reason',
         when(col('product_id').isNull() | (trim(col('product_id')) == ""), 'Missing product_id')
        .when(~col("product_id").startswith("PROD") | (length(col("product_id")) != 10), 'Invalid product_id')
        .otherwise(col('drop_reason'))
    )
    product_window = Window.partitionBy(col('product_id')).orderBy(col('updated_at').desc())

    df= df.withColumn(
        'product_rn',
        row_number().over(product_window)
    ).withColumn(
        'drop_reason',
        when(col('product_rn') > 1, lit('duplicate_product_id'))
        .otherwise(col('drop_reason'))
    )

    # fixing null category id, supplier id,mrp, selling price, cost price
    df = df.withColumn(
        'drop_reason',
        when(col('category_id').isNull() | (trim(col('category_id')) == ""), 'Missing category_id')
        .when(col('supplier_id').isNull() | (trim(col('supplier_id')) == ""), 'Invalid supplier_id')
        .when(col('mrp') < 0 , lit('Negative mrp'))
        .when(col('selling_price') < 0, lit('Negative selling_price'))
        .when(col('cost_price') < 0 , lit('Negative cost_price'))
        .otherwise(col('drop_reason'))
    )

    # fixing invalid product rating
    df = df.withColumn(
        'product_rating',
         when((col('product_rating') < 0) | (col('product_rating') > 5), lit(None))
        .otherwise(col('product_rating'))
    )

    df = df.withColumn(
        'total_reviews',
        when(col('total_reviews') < 0, lit(0))
        .otherwise(col('total_reviews'))
    )

    # fixing launch date
    df = df.withColumn(
        'drop_reason',
        when(col('launch_date') > current_date(), lit('Future launch date'))
        .otherwise(col('drop_reason'))
    )

    # warning for duplicate SKU
    sku_window = Window.partitionBy(col('sku')).orderBy('updated_at')
    df = df.withColumn(
        'sku_count',
        when(col('sku').isNotNull(),count('sku').over(sku_window))
        .otherwise(lit(0))
    )

    df = df.withColumn(
        'sku_duplicate_flag',
        when(col('sku_count') > 1 , lit(True)).otherwise(lit(False))
    )

    # if deleted_flag is true, make product status discontinued
    df = df.withColumn(
        "product_status",
        when(col("deleted_flag") == True, lit("DISCONTINUED"))
        .otherwise(col("product_status"))
    )

    df = df.withColumn("silver_processed_timestamp", current_timestamp())

    bad_records = (
        df.filter(col("drop_reason").isNotNull())
        .withColumnRenamed("drop_reason", "rejection_reason")
        .withColumn("quarantine_timestamp", current_timestamp())
        .drop("product_rn", "sku_count")
    )

    clean_df = (
        df.filter(col("drop_reason").isNull())
        .drop("drop_reason", "product_rn", "sku_count")
    )

    return clean_df, bad_records



