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

def product_performance_mart(
        spark,
        logger,
        quality_path,
        gold_path
):
    try:
        logger.info(' Starting product_performance_mart ')

        df = spark.read.option("inferSchema", "true").option("header", "true").csv(f'{gold_path}/fact_sales')
        reviews = spark.read.parquet(f'{quality_path}/reviews')

        product_df =(
            df.join(reviews.select('customer_id','product_id','rating').alias('R'),['customer_id','product_id'],'left')
            .drop('R.product_id','R.customer_id')
            .groupBy(
                'product_id','brand','category'
            )
            .agg(
                count_distinct('order_id').alias('Total orders'),
                sum('quantity').alias('Units sold'),
                sum('price').cast(DecimalType(18, 2)).alias('Total revenue'),
                round(avg('price'),2).alias('Avg selling price'),
                round(avg('rating'),2).alias('Avg rating')
            )
        )
        revenue_window = Window.partitionBy('category').orderBy(desc('Total revenue'))

        product_df = (
            product_df.withColumn(
                'rank_in_category',
                row_number().over(revenue_window)
            ).withColumn(
                'product_segment',
                when(col('Total revenue') > 1000000 , 'Star')
                .when((col('Total revenue') < 100000) & (col('Total revenue') > 100000) , 'Average')
                .otherwise(lit('low'))
            ).orderBy('category','rank_in_category')
        )
        write_single_csv(product_df, f"{gold_path}/product_performance_mart", 'product_performance_mart.csv')

        # product_df.show(20,truncate=False)
        logger.info(
            "Completed product_performance_mart"
        )

        return True

    except Exception as e:
        logger.error(
            f"product_performance_mart failed: {str(e)}"
        )
        return False




