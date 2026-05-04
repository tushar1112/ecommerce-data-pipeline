import socketserver
import os

# empty object for unix features (error handle)
if os.name == 'nt':
    socketserver.UnixStreamServer = object
    socketserver.UnixDatagramServer = object

from pyspark.sql import Window
from pyspark.sql.functions import *

def transform_orders(df):
    #1. fix invalid order_id,customer_id
    df = df.withColumn(
        'Drop_reason',
        when(col('order_id').isNull(),'Blank order id')\
        .when(~col('order_id').startswith('ORD') | (length(col('order_id'))!=9),'Invalid order id') \
        .when(col("customer_id").isNull() | (~col("customer_id").startswith("CUST")) |
                 (length(col("customer_id")) != 10), 'Invalid customer ID')
        .otherwise(lit(None))
    )
    #2. fixing duplicate payment ids
    window = Window.partitionBy('payment_id').orderBy(col('order_id').asc())
    df= df.withColumn(
        'ranking_payment',
        row_number().over(window)
    ).withColumn(
        'Drop_reason',
        when(col('ranking_payment')>1,'Duplicate payment id')\
        .otherwise(col('Drop_reason'))
    )

    #3. fixing issues in dates - order-shipment-delivery
    df = df.withColumn('purchase_date',try_to_date(col('purchase_date'),'dd-MM-yyyy'))\
            .withColumn('shipment_date',try_to_date(col('shipment_date'),'dd-MM-yyyy'))\
            .withColumn('delivery_date',try_to_date(col('delivery_date'),'dd-MM-yyyy'))
    df = df.withColumn(
        'Drop_reason',
        when(col('Drop_reason').isNotNull(),col('Drop_reason'))\
        .when(col('purchase_date').isNull() ,'missing purchase date')\
        .when(col('shipment_date').isNull(),'missing shipment date')\
        .when(col('delivery_date').isNull(),'missing delivery date')\
        .when(col('shipment_date') < col('purchase_date'),'shipment date issue')\
        .when(col('delivery_date') < col('shipment_date'),'delivery date issue')
    )

    bad_records = df.filter(col('Drop_reason').isNotNull())  # bad record
    df = df.filter(col('Drop_reason').isNull()).drop('Drop_reason','ranking_payment') # good record

    return df, bad_records