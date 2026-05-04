import socketserver
import os

from pyspark.sql import Window
from pyspark.sql.functions import *

# empty object for unix features (error handle)
if os.name == 'nt':
    socketserver.UnixStreamServer = object
    socketserver.UnixDatagramServer = object

#-----------------------------  code  ---------------------------------

def transform_reviews(df):

    #1. fixing invalid/null review ids, customer_id, seller_id, product_id
    df = df.withColumn(
        'Drop_reason',
        when(col('review_id').isNull() | (~col('review_id').startswith('REV')) | (length(col('review_id')) != 10),
             'review id is null/invalid')\
        .when(col('customer_id').isNull() | (~col('customer_id').startswith('CUST')) | (length(col('customer_id')) != 10),
             'customer id is null/invalid') \
        .when((~col('seller_id').startswith('SELL')) | (length(col('seller_id')) != 9),
            'seller is invalid')
        .when(col('product_id').isNull() | (~col('product_id').startswith('PROD')) | (length(col('product_id')) != 10),
            'Product id is invalid')
        .otherwise(lit(None))
    )

    #2. fixing invalid rating

    _reg_ex = r'[^1-5]'
    df = df.withColumn(
        'rating',
        when(col('rating').isNull() | (col('rating').rlike(_reg_ex)), '1')
        .otherwise(col('rating'))
    )


    bad_records = df.filter(col('Drop_reason').isNotNull())  # bad record
    df = df.filter(col('Drop_reason').isNull()).drop('Drop_reason') # good record

    return df, bad_records

