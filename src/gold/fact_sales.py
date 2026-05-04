from pyspark.sql.functions import *
from pyspark.sql import Window
from src.silver import orders, payments
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

def build_fact_sales(
        spark,
        logger,
        quality_path,
        gold_path
):
    try:
        logger.info('Started fact sales')
        orders = spark.read.parquet(f'{quality_path}/orders')
        order_details = spark.read.parquet(f'{quality_path}/order_details')
        products = spark.read.parquet(f'{quality_path}/products')
        payments = spark.read.parquet(f'{quality_path}/payments')

        df = (
            order_details.alias('OD')
            .join(
                orders.alias('O'),
                'order_id','inner'
            )
            .join(
                products.alias('P'),
                'product_id','left'
            )
            .join(
                payments.alias('pay'),
                'payment_id','left'
            )
        )

        fact_sales = (
            df.withColumn('sales_date',try_to_date('purchase_date'))
                .withColumn('sales_month',date_format('purchase_date','MMMM'))
                .withColumn('sales_year',date_format('purchase_date','yyyy'))
                .withColumn('gross_sales',round(col('quantity')*col('price'),2))
                .select(
                "OD.order_id",
                "OD.product_id",
                "O.customer_id",
                "OD.seller_id",
                "O.payment_id",
                "sales_date",
                "sales_month",
                "sales_year",
                "category",
                "brand",
                "quantity",
                "price",
                "gross_sales",
                "payment_amount"
            )
        )


        # fact_sales.show(20,truncate=False)

        write_single_csv(fact_sales, f"{gold_path}/fact_sales", 'fact_sales.csv')

        logger.info(
            "Completed fact_sales"
        )

        return True

    except Exception as e:
        logger.error(
            f"fact_sales failed: {str(e)}"
        )
        return False

