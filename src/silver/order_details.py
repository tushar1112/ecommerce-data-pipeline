import socketserver
import os

# empty object for unix features (error handle)
if os.name == 'nt':
    socketserver.UnixStreamServer = object
    socketserver.UnixDatagramServer = object

from pyspark.sql import Window
from pyspark.sql.functions import *

#-----------------------------  code  ---------------------------------

def transform_order_details(df):

    #filtering null values at orderid, product id and seller id
    df = df.withColumn(
        'Drop_reason',
        when(col('order_id').isNull() | (trim(col("order_id")) == ""),'missing order id')\
        .when(col('product_id').isNull() | (trim(col("product_id")) == ""),'missing product id')\
        .when(col('seller_id').isNull() | (trim(col("seller_id")) == ""), 'missing seller id')\
        .otherwise(lit(None))
        )
    # invalid orderid, product id and seller id
    df = df.withColumn(
        'Drop_reason',
        when(col('Drop_reason').isNotNull(), col('Drop_reason'))\
        .when((~col('order_id').startswith('ORD')) | (length(col('order_id'))!=9), 'Invalid order id') \
        .when((~col('product_id').startswith('PROD')) | (length(col('product_id'))!=10), 'Invalid product id') \
        .when((~col('seller_id').startswith('SELL')) | (length(col('seller_id'))!=9), 'Invalid seller id')
    )

    #fixing invalid price, price per unit and total amount

    _reg_ex = r'[^0-9.]'
    df = df.withColumn(
        'Drop_reason',
        when(col('Drop_reason').isNotNull(), col('Drop_reason')) \
        .when(col('quantity').isNotNull() & (col('quantity').rlike(_reg_ex)),'Invalid quantity_int')\
        .when(col('price_per_unit').isNotNull() & (col('price_per_unit').rlike(_reg_ex)),'Invalid price_float')\
        .when(col('total_amount').isNotNull() & (col('total_amount').rlike(_reg_ex)),'Invalid total_amount_float')
    )

    df = df.withColumn('quantity_int', (when(col('Drop_reason').isNull(),col("quantity").try_cast('integer')).otherwise(lit(None))))\
        .withColumn('price_float', (when(col('Drop_reason').isNull(),col("price_per_unit").try_cast('decimal(18,2)')).otherwise(lit(None))))\
        .withColumn('total_amount_float',(when(col('Drop_reason').isNull(),col("total_amount").try_cast('decimal(18,2)')).otherwise(lit(None))))

    df = df.withColumn(
        'Drop_reason',
         when(col('Drop_reason').isNotNull(),col('Drop_reason'))\
         .when(col('quantity_int').isNull() | (col('quantity_int') <= 0), 'Invalid quantity')\
         .when(col('price_float').isNull() | (col('price_float') <= 0), 'Invalid price per unit')\
         .when(col('total_amount_float').isNull() | (col('total_amount_float') <= 0), 'Invalid total amount')\
    )

    df = df.drop('quantity_int','price_float','total_amount_float')
    bad_records = df.filter(col('Drop_reason').isNotNull())  # bad record
    df = df.filter(col('Drop_reason').isNull()).drop('Drop_reason')  # good record

    return df, bad_records







