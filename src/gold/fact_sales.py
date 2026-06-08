from pyspark.sql.functions import *

def build_fact_sales(order_df, order_item_df, dim_customer, dim_products):

    fact_sales = order_item_df.alias('oi').join(
        order_df.alias('o'),
        col('oi.order_id')==col('o.order_id'),
        'inner'
    ).join(
        dim_customer.alias('c').select("c.customer_sk", "c.customer_id"),
        col('c.customer_id')==col('o.customer_id'),
        'left'
    ).join(
        dim_products.alias('p').select("p.product_sk","p.product_id","p.category_id","p.supplier_id"),
        col('p.product_id') == col('oi.product_id'),
        'left'
    )

    fact_sales = fact_sales.withColumn(
        'date_key',
        date_format(
            col('o.order_date'),'yyyyMMdd').cast('int')
        )


    fact_sales = fact_sales.select(
        "date_key",
        "customer_sk",
        "product_sk",

        col("o.order_id"),
        col("oi.order_item_id"),

        col("oi.quantity"),
        col("oi.unit_price"),

        col("oi.gross_amount"),
        col("oi.discount_amount"),
        col("oi.final_price"),

        col("oi.estimated_shipping_cost"),

        col("o.order_status"),
        col("o.payment_status"),

        col("oi.item_status"),

        col("o.order_source"),
        col("o.device_type"),

        col('p.category_id'),
        col('p.supplier_id'),
        col("oi.warehouse_id")
    )

    return fact_sales


def build_fact_sales_inc(silver_order_df, silver_order_item_df, dim_customer, dim_products, gold_dim_fact_sales):

    # get changed/modifed order_id from silver orders and silver order_items
    latest_order_batch = (
        silver_order_df
        .filter(col("operation_type").isin("INSERT", "UPDATE", "DELETE"))
        .select("batch_id")
        .orderBy(col("ingestion_timestamp").desc())
        .first()[0]
    )

    latest_item_batch = (
        silver_order_item_df
        .filter(col("operation_type").isin("INSERT", "UPDATE", "DELETE"))
        .select("batch_id")
        .orderBy(col("ingestion_timestamp").desc())
        .first()[0]
    )

    changed_order_ids = (
        silver_order_df.filter(col("batch_id") == latest_order_batch).select("order_id")
        .unionByName(
            silver_order_item_df.filter(col("batch_id") == latest_item_batch).select("order_id")
        )
        .distinct()
    )

    # getting unchanged fact sales( use to merge it as it is in final_df)

    unchanged_fact_sales = gold_dim_fact_sales.join(
        changed_order_ids,
        on="order_id",
        how="left_anti"
    )

    #Rebuild Only Changed Orders

    affected_orders = (
        silver_order_df
        .join(
            changed_order_ids,
            on="order_id",
            how="inner"
        )
    )

    affected_order_items = (
        silver_order_item_df
        .join(
            changed_order_ids,
            on="order_id",
            how="inner"
        )
    )

    # want uniqe ids because SCD2 has been applied here, which will create duplicate later in build_fact_sales joins
    dim_customer_current = dim_customer.filter(col("is_current") == True)
    dim_product_current = dim_products.filter(col("is_current") == True)

    updated_fact_sales_records = build_fact_sales(affected_orders,affected_order_items,dim_customer_current, dim_product_current)

    final_dim_fact_sales = unchanged_fact_sales.unionByName(updated_fact_sales_records,allowMissingColumns=True)

    print(f'unchanged_fact_sales rows count :{unchanged_fact_sales.count()}')
    print(f'changed_order_ids  count :{changed_order_ids.count()}')
    print(f'affected_orders  count :{affected_orders.count()}')
    print(f'affected_order_items  count :{affected_order_items.count()}')
    print(f'updated_fact_sales_records  count :{updated_fact_sales_records.count()}')
    print(f' final_dim_fact_sales count : {final_dim_fact_sales.count()}')

    return final_dim_fact_sales





