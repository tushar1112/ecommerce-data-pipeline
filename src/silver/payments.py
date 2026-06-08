from pyspark.sql import Window
from pyspark.sql.functions import *

def transform_payments(df):
    column_type_mapping = {
        "payment_id": "string",
        "order_id": "string",
        "customer_id": "string",
        "payment_date": "timestamp",
        "payment_method": "string",
        "payment_provider": "string",
        "payment_amount": "double",
        "currency": "string",
        "payment_status": "string",
        "transaction_reference_id": "string",
        "refund_amount": "double",
        "refund_date": "timestamp",
        "created_at": "timestamp",
        "updated_at": "timestamp",
        "deleted_flag": "boolean",
        "source_system": "string",
        "record_version": "int",
        # Bronze metadata
        "source_table": "string",
        "ingestion_timestamp": "timestamp",
        "ingestion_date": "date",
        "batch_id": "string",
        "operation_type": "string"
    }
    for column_name , data_type in column_type_mapping.items():
        df = df.withColumn(column_name,col(column_name).cast(data_type))

    # drop_reason column
    df = df.withColumn("drop_reason", lit(None).cast('string'))

    #1. fix invalid payment_id, order_id
    df = df.withColumn(
        'drop_reason',
         when((col('payment_id').isNull()) | (~col('payment_id').startswith('PAY')),lit('Invalid payment_id'))
        .otherwise(col('drop_reason'))
    )
    pay_window = Window.partitionBy('payment_id').orderBy(col('updated_at').desc())

    df = df.withColumn(
        'pay_rn',
        row_number().over(pay_window)
    ).withColumn(
        'drop_reason',
        when(col('pay_rn') > 1, lit('duplicate payment id'))
        .otherwise(col('drop_reason'))
    )


    df = df.withColumn(
        'drop_reason',
        when(col('order_id').isNull(), lit('Invalid order id'))
        .when(col('customer_id').isNull(), 'Invalid customer id')
        .otherwise(col('drop_reason'))
    )

    # negative payment amount & refund amount, invalid dates
    df = df.withColumn(
        'drop_reason',
        when(col('payment_amount') < 0, lit('negative payment amount'))
        .when(col('refund_amount') < 0, lit('negative refund amount'))
        .when(col('payment_date') > current_timestamp(),lit('future_payment_date'))
        .when(col('refund_date') < col('payment_date'),lit('invalid refund date'))
        .otherwise(col('drop_reason'))
    )

   #3. fixing payment mode
    valid_modes = ["UPI", "CREDIT_CARD","DEBIT_CARD","NET_BANKING", "CASH_ON_DELIVERY", "WALLET"]
    df = df.withColumn(
        'payment_method',
        when(col('payment_method').isin(valid_modes),col('payment_method'))
        .otherwise(lit('UNKNOWN'))
    )

    #4 flag for blank transaction id
    df = df.withColumn(
        'blank_transaction_id_flag',
        when(col('transaction_reference_id').isNull(),lit(True))
        .otherwise(lit(False))
    )

    df = df.withColumn(
        'silver_processed_timestamp', current_timestamp()
    )

    bad_records = (
        df.filter(col("drop_reason").isNotNull())
        .withColumnRenamed("drop_reason", "rejection_reason")
        .withColumn("quarantine_timestamp", current_timestamp())
        .drop("pay_rn")
    )

    clean_df = (
        df.filter(col("drop_reason").isNull())
        .drop("pay_rn", "drop_reason")
    )

    return clean_df, bad_records