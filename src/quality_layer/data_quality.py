from pyspark.sql.functions import *

def read_data(spark,path):
    return spark.read.parquet(path)

def write_data(df,path):
    df.write \
        .mode("overwrite") \
        .parquet(path)

#separating valid and invalid data at different locations
def split_and_save(
    df,
    clean_path,
    reject_path
):
    valid_df = df.filter(col("Drop_reason").isNull())
    invalid_df = df.filter(col("Drop_reason").isNotNull())
    valid_df = valid_df.drop("Drop_reason")

    write_data(valid_df, clean_path)
    write_data(invalid_df, reject_path)


def vaild_orders(orders,customers,payments,good_path,reject_path):
    df = orders.alias('o')\
            .join(customers.select(col('customer_id').alias('cust_customer_id')),col('o.customer_id') == col('cust_customer_id'),'left')\
            .join(payments.select(col('payment_id').alias('pay_payment_id')),col('o.payment_id') == col('pay_payment_id'),'left')\
            .withColumn(
             'Drop_reason',
             when(col('cust_customer_id').isNull(), 'referential integrity issue for customer id')\
             .when(col('pay_payment_id').isNull(),'referential integrity issue for payment id')\
             .otherwise(lit(None))
    )
    df = df.drop('cust_customer_id','pay_payment_id')
    split_and_save(df,good_path,reject_path)

def valid_order_items(order_details,products,orders,sellers,good_path,reject_path):
    df = (order_details.alias('od')
            .join(orders.select(col('order_id').alias('order_order_id')),col('od.order_id')==col('order_order_id'),'left')
            .join(products.select(col('product_id').alias('prod_product_id')),col('od.product_id')==col('prod_product_id'),'left')
            .join(sellers.select(col('seller_id').alias('sell_seller_id')),col('od.seller_id')==col('sell_seller_id'),'left')
          .withColumn(
            'Drop_reason',
            when(col('order_order_id').isNull(), 'referential integrity issue for order id')
            .when(col('prod_product_id').isNull(),'referential integrity issue for product id')
            .when(col('sell_seller_id').isNull(),'referential integrity issue for seller id')
            .otherwise(lit(None))
            )
          )
    df = df.drop('order_order_id', 'prod_product_id','sell_seller_id')
    split_and_save(df, good_path, reject_path)

def valid_payments(payment,orders,good_path,reject_path):
    df = payment.alias('p')\
            .join(orders.select(col('order_id').alias('ord_order_id')),col('p.order_id')==col('ord_order_id'),'left')\
            .withColumn(
            'Drop_reason',
             when(col('ord_order_id').isNull(), 'referential integrity issue for order id')\
            .otherwise(lit(None))
        )
    df = df.drop('ord_order_id')
    split_and_save(df, good_path, reject_path)

def valid_reviews(review,customers,sellers,products,good_path,reject_path):
    df = (review.alias('r')
        .join(customers.select(col('customer_id').alias('cust_customer_id')), col('r.customer_id') == col('cust_customer_id'),
              'left')
        .join(products.select(col('product_id').alias('prod_product_id')),
              col('r.product_id') == col('prod_product_id'), 'left')
        .join(sellers.select(col('seller_id').alias('sell_seller_id')), col('r.seller_id') == col('sell_seller_id'),
              'left')
        .withColumn(
        'Drop_reason',
        when(col('cust_customer_id').isNull(), 'referential integrity issue for customer id')
            .when(col('prod_product_id').isNull(), 'referential integrity issue for product id')
            .when(col('sell_seller_id').isNull(), 'referential integrity issue for seller id')
            .otherwise(lit(None))
        )
    )
    df = df.drop('cust_customer_id','prod_product_id','sell_seller_id')
    split_and_save(df, good_path, reject_path)

def run_quality_checks(
        spark,
        logger,
        silver_path,
        quality_clean_path,
        quality_reject_path
):
    #we will be checking referential integrity among all the tables. if mismatch found, put it seperate in reject_path
    try:
        # Read silver data
        customers = read_data(spark,f'{silver_path}/customers')
        products = read_data(spark,f'{silver_path}/products')
        orders = read_data(spark,f'{silver_path}/orders')
        sellers = read_data(spark,f'{silver_path}/sellers')
        order_details = read_data(spark,f'{silver_path}/order_details')
        payments = read_data(spark,f'{silver_path}/payments')
        reviews = read_data(spark,f'{silver_path}/reviews')

        # checking their validity
        vaild_orders(orders,customers,payments,f'{quality_clean_path}/orders',f'{quality_reject_path}/orders')
        valid_order_items(order_details,products,orders,sellers,f'{quality_clean_path}/order_details',f'{quality_reject_path}/order_details')
        valid_payments(payments,orders,f'{quality_clean_path}/payments',f'{quality_reject_path}/payments')
        valid_reviews(reviews,customers,sellers,products,f'{quality_clean_path}/reviews',f'{quality_reject_path}/reviews')
        #writing them as it is in quality layer
        write_data(customers,f'{quality_clean_path}/customers')
        write_data(products,f'{quality_clean_path}/products')
        write_data(sellers,f'{quality_clean_path}/sellers')

    except Exception as e:
        logger.error(e)