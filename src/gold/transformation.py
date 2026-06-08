from src.common import watermark
from src.common.watermark import get_last_processed_batch, update_watermark
from src.gold import fact_payments
from src.gold.dim_customer import build_dim_customer,build_dim_customers_inc
from src.gold.dim_products import build_dim_products,build_dim_products_inc
from src.gold.dim_date import build_dim_date
from src.gold.fact_payments import build_fact_payments, build_fact_payments_inc
from src.gold.fact_sales import build_fact_sales, build_fact_sales_inc


def gold_transformations(spark,logger,config):

    silver_path = config["paths"]["silver"]
    gold_path = config["paths"]["gold"]
    load_type = config["load_type"]
    watermark_path = config["paths"]["watermark"]
    BI_path = config["paths"]["dashboard"]
    # reading all necessary silver files
    silver_customer_df = spark.read.parquet(f"{silver_path}/customers")
    silver_product_df = spark.read.parquet(f"{silver_path}/products")
    silver_supplier_df = spark.read.parquet(f"{silver_path}/suppliers")
    silver_orders_df = spark.read.parquet(f"{silver_path}/orders")
    silver_order_items_df = spark.read.parquet(f"{silver_path}/order_items")
    silver_payments_df = spark.read.parquet(f"{silver_path}/payments")
    silver_category_df = spark.read.parquet(f"{silver_path}/categories")

    silver_success_batch_customer = get_last_processed_batch(
        spark,
        watermark_path,
        table_name="customers",
        layer="silver"
    )
    silver_success_batch_products = get_last_processed_batch(
        spark,
        watermark_path,
        table_name="products",
        layer="silver"
    )

    silver_success_batch_order_items = get_last_processed_batch(
        spark,
        watermark_path,
        table_name="order_items",
        layer="silver"
    )

    silver_success_batch_payments = get_last_processed_batch(
        spark,
        watermark_path,
        table_name="payments",
        layer="silver"
    )

    if load_type == "FULL_LOAD":
        # 1. creating dimension & fact tables
        dim_customer = build_dim_customer(silver_customer_df)
        dim_products = build_dim_products(silver_product_df, silver_category_df, silver_supplier_df)
        dim_date = build_dim_date(spark, start_date='2020-01-01', end_date='2030-12-31')
        fact_sales = build_fact_sales(silver_orders_df, silver_order_items_df, dim_customer, dim_products)
        fact_payments = build_fact_payments(silver_payments_df,dim_customer)

        #2. Writing dimension and fact tables
        dim_customer.write.mode("overwrite").parquet(f"{gold_path}/dim_customer")
        dim_products.write.mode("overwrite").parquet(f"{gold_path}/dim_products")
        dim_date.write.mode("overwrite").parquet(f"{gold_path}/dim_date")
        fact_sales.write.mode("overwrite").parquet(f"{gold_path}/fact_sales")
        fact_payments.write.mode("overwrite").parquet(f"{gold_path}/fact_payments")

        #3. printing total
        print(f" silver_customer_df count: {silver_customer_df.count()}")
        print(f" silver_product_df count: {silver_product_df.count()}")
        print(f" silver_order_items count:  {silver_order_items_df.count()}")
        print(f" silver_payments_df count: {silver_payments_df.count()}")

        print(f" dim_customer count: {dim_customer.count()}")
        print(f" dim_product count: {dim_products.count()}")
        print(f" dim_date count:  {dim_date.count()}")
        print(f" fact_sales count:  {fact_sales.count()}")
        print(f" fact_payments count: {fact_payments.count()}")

    else:
        gold_dim_customer = spark.read.parquet(f"{gold_path}/dim_customer")
        dim_customer = build_dim_customers_inc(silver_customer_df,gold_dim_customer)
        dim_customer.write.mode('overwrite').parquet(f"{gold_path}/temp")
        new_df = spark.read.parquet(f"{gold_path}/temp")
        new_df.write.mode('overwrite').parquet(f"{gold_path}/dim_customer")
        update_watermark(
            spark,
            watermark_path,
            table_name="dim_customer",
            layer="gold",
            batch_id=silver_success_batch_customer,
            status="SUCCESS"
        )

        gold_dim_products = spark.read.parquet(f"{gold_path}/dim_products")
        dim_products = build_dim_products_inc(silver_product_df,gold_dim_products,silver_category_df,silver_supplier_df)
        dim_products.write.mode('overwrite').parquet(f"{gold_path}/temp")
        new_df = spark.read.parquet(f"{gold_path}/temp")
        new_df.write.mode('overwrite').parquet(f"{gold_path}/dim_products")
        update_watermark(
            spark,
            watermark_path,
            table_name="dim_products",
            layer="gold",
            batch_id=silver_success_batch_products,
            status="SUCCESS"
        )

        dim_customer = spark.read.parquet(f"{gold_path}/dim_customer")
        dim_products = spark.read.parquet(f"{gold_path}/dim_products")
        dim_fact_sales = spark.read.parquet(f"{gold_path}/fact_sales")
        updated_fact_sales = build_fact_sales_inc(silver_orders_df, silver_order_items_df, dim_customer, dim_products,dim_fact_sales)
        updated_fact_sales.write.mode('overwrite').parquet(f"{gold_path}/temp")
        new_df = spark.read.parquet(f"{gold_path}/temp")
        new_df.write.mode('overwrite').parquet(f"{gold_path}/fact_sales")
        update_watermark(
            spark,
            watermark_path,
            table_name="fact_sales",
            layer="gold",
            batch_id=silver_success_batch_order_items,
            status="SUCCESS"
        )

        dim_fact_payments = spark.read.parquet(f"{gold_path}/fact_payments")
        updated_fact_payments = build_fact_payments_inc(silver_payments_df,dim_customer,dim_fact_payments)
        updated_fact_payments.write.mode('overwrite').parquet(f"{gold_path}/temp")
        new_df = spark.read.parquet(f"{gold_path}/temp")
        new_df.write.mode('overwrite').parquet(f"{gold_path}/fact_payments")
        update_watermark(
            spark,
            watermark_path,
            table_name="fact_payments",
            layer="gold",
            batch_id=silver_success_batch_payments,
            status="SUCCESS"
        )

        # data export for power_BI
        dim_cust = spark.read.parquet(f"{gold_path}/dim_customer")
        dim_prod = spark.read.parquet(f"{gold_path}/dim_products")
        dim_fact = spark.read.parquet(f"{gold_path}/fact_sales")
        dim_fact_pay = spark.read.parquet(f"{gold_path}/fact_payments")
        dim_date = spark.read.parquet(f"{gold_path}/dim_date")
        dim_cust.coalesce(1).write.mode('overwrite').options(header=True,inferSchema=True).csv(f"{BI_path}/dim_customer")
        dim_prod.coalesce(1).write.mode('overwrite').options(header=True, inferSchema=True).csv(f"{BI_path}/dim_products")
        dim_fact.coalesce(1).write.mode('overwrite').options(header=True, inferSchema=True).csv(f"{BI_path}/fact_sales")
        dim_fact_pay.coalesce(1).write.mode('overwrite').options(header=True, inferSchema=True).csv(f"{BI_path}/fact_payments")
        dim_date.coalesce(1).write.mode('overwrite').options(header=True, inferSchema=True).csv(
            f"{BI_path}/dim_date")










