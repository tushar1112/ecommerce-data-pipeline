from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import DecimalType
import os
import shutil

def write_single_csv(df, output_path, final_name):
    temp_path = output_path + "_temp"

    # Write single file
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(temp_path)

    # Find generated file
    file = [f for f in os.listdir(temp_path) if f.startswith("part-")][0]

    # Move & rename
    shutil.move(os.path.join(temp_path, file),
                os.path.join(output_path, final_name))

    # Delete temp folder
    shutil.rmtree(temp_path)

def sellers_performance_mart(
        spark,
        logger,
        quality_path,
        gold_path
):
    try:
        logger.info(' Starting seller_performance_mart ')

        df = spark.read.option("inferSchema", "true").option("header", "true")\
        .csv(f'{gold_path}/fact_sales')
        reviews = spark.read.parquet(f'{quality_path}/reviews')
        sellers = spark.read.parquet(f'{quality_path}/sellers')

        seller_df =(
            df.join(reviews.select('customer_id','product_id','seller_id','rating').alias('R'),['customer_id','product_id','seller_id'],'left')
            .join(sellers.select('seller_id','seller_name','state').alias('S'),'seller_id','left')
            .drop('R.product_id','R.customer_id','R.seller_id','S.seller_id')
            .groupBy(
                'sales_year','sales_month','seller_id','seller_name','state'
            )
            .agg(
                count_distinct('order_id').alias('Total orders'),
                sum('quantity').alias('Units sold'),
                sum('price').cast(DecimalType(18, 2)).alias('Total revenue'),
                round(avg('rating'),2).alias('Avg rating')
            )
        )

        revenue_window = Window.partitionBy('state').orderBy(desc('Total revenue'))

        seller_df = (
            seller_df.withColumn(
                'rank_in_state',
                row_number().over(revenue_window)
            ).withColumn(
                'seller_segment',
                when(col('Total revenue') > 1000000 , 'Top')
                .when((col('Total revenue') < 100000) & (col('Total revenue') > 50000) , 'Average')
                .otherwise(lit('low'))
            ).orderBy('sales_year','sales_month','rank_in_state','Avg rating')
        )

        write_single_csv(seller_df, f"{gold_path}/sellers_performance_mart", 'sellers_performance_mart.csv')

        # seller_df.show(20,truncate=False)
        logger.info(
            "Completed sellers_performance_mart"
        )

        return True

    except Exception as e:
        logger.error(
            f"sellers_performance_mart failed: {str(e)}"
        )
        return False




