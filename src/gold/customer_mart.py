from pyspark.sql.functions import *
from pyspark.sql import Window
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

def customer_mart(
        spark,
        logger,
        gold_path
):
    try:
        logger.info(' Starting customer_mart ')

        df = spark.read.option("inferSchema", "true").option("header", "true").csv(f'{gold_path}/fact_sales')

        today_date = current_date()

        customer_mart = (
            df.groupBy('customer_id')
            .agg(
                min('sales_date').alias('first_purchase_date'),
                max('sales_date').alias('last_purchase_date'),
                count_distinct('order_id').alias('total_orders'),
                count_distinct('product_id').alias('total_unique_products_bought'),
                sum('quantity').alias('total_units_bought'),
                round(sum('gross_sales'),2).alias('total_spending'),
                round(avg('gross_sales'),2).alias('average_order_value')
            )
            .withColumn('days_since_last_order',datediff(today_date,'last_purchase_date'))
            .withColumn('customer_segment',
                        when(col('total_spending') > 5000000, 'VIP')
                        .when((col('total_spending') > 200000) & (col('total_spending') < 5000000), 'Important')
                        .otherwise('Regular')
                        )
            .orderBy(desc('total_spending'))
        )


        write_single_csv(customer_mart,f"{gold_path}/customer_mart",'customer_mart.csv')

        logger.info(
            "Completed customer_360_mart"
        )
        return True

    except Exception as e:
        logger.error(f'Customer_mart failed : {str(e)}')
        return False

