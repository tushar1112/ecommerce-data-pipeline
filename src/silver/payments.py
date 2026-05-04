import socketserver
import os

# empty object for unix features (error handle)
if os.name == 'nt':
    socketserver.UnixStreamServer = object
    socketserver.UnixDatagramServer = object

from pyspark.sql import Window
from pyspark.sql.functions import *

def transform_payments(df):
    #1. fix invalid payment_id, order_id
    df = df.withColumn(
        'Drop_reason',
         when(col('order_id').isNull() | (col('payment_id').isNull()),'Blank order id/payment_id')\
        .when(~col('order_id').startswith('ORD') | (length(col('order_id'))!=9),'Invalid order id') \
        .when(~col('payment_id').startswith('PAY') | (length(col('payment_id'))!=9),'Invalid payment id') \
        .otherwise(lit(None))
    )
    #2. fixing duplicate payment ids
    window = Window.partitionBy('payment_id').orderBy(col('order_id').asc())
    df= df.withColumn(
        'ranking_payment',
        row_number().over(window)
    ).withColumn(
        'Drop_reason',
         when(col('Drop_reason').isNotNull(),col('Drop_reason'))\
        .when(col('ranking_payment')>1,'Duplicate payment id')
    )

   #3. fixing payment mode
    valid_modes = ["UPI", "Credit Card", "Debit Card","Net Banking", "Cash on Delivery", "Wallet"]
    df = df.withColumn(
        'payment_mode',
        when(col('payment_mode').isin(valid_modes),col('payment_mode'))
        .otherwise(lit('other'))
    )
    #4. fixing payment date
    df = (df.withColumnRenamed('payment_time','payment_date')
            .withColumn('payment_date',try_to_timestamp(col('payment_date'), lit('dd-MM-yyyy HH:mm')).try_cast('date'))
            .withColumn(
                'Drop_reason',
                when(col('Drop_reason').isNotNull(),col('Drop_reason'))
                .when(col('payment_date') > current_date(),'future_payment_date')
            ))

    # df.select('payment_date').distinct().show()

    df = df.drop('ranking_payment')

    # fixing payment amount
    df = df.withColumn(
        'payment_amount',
        col('payment_amount').try_cast('decimal(18,2)')
    ).withColumn(
        'Drop_reason',
        when(col('payment_amount').isNull() | (col('payment_amount') <= 0),'invalid payment amount')
    )

    bad_records = df.filter(col('Drop_reason').isNotNull())  # bad record
    df = df.filter(col('Drop_reason').isNull()).drop('Drop_reason') # good record


    return df, bad_records