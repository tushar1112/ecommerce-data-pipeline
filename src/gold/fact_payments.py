from pyspark.sql.functions import *


def build_fact_payments( payments_df, dim_customer_df):

    fact_payments = payments_df.alias('p').join(
        dim_customer_df.alias('c').select('customer_sk', 'customer_id'),
        on='customer_id',
        how='left'
    )

    fact_payments = fact_payments.withColumn(
        'date_key',
        date_format(col('p.payment_date'),'yyyyMMdd').cast('int')
    )

    fact_payments = fact_payments.select(
        'date_key',
        'customer_sk',
        col('p.payment_id'),
        col("p.order_id"),

        col("p.transaction_reference_id"),

        col("p.payment_amount"),
        col("p.refund_amount"),

        col("p.currency"),

        col("p.payment_method"),
        col("p.payment_provider"),

        col("p.payment_status")
    )

    return fact_payments


def build_fact_payments_inc(silver_payments_df, dim_customer, dim_fact_payments):
    # get changed/modifed payments_id from silver payments
    latest_order_batch = (
        silver_payments_df
        .filter(col("operation_type").isin("INSERT", "UPDATE", "DELETE"))
        .select("batch_id")
        .orderBy(col("ingestion_timestamp").desc())
        .first()[0]
    )

    changed_payment_ids = (
        silver_payments_df.filter(col("batch_id") == latest_order_batch).select("payment_id")
        .distinct()
    )

    # getting unchanged fact sales( use to merge it as it is in final_df)

    unchanged_fact_payments = dim_fact_payments.join(
        changed_payment_ids,
        on="payment_id",
        how="left_anti"
    )

    affected_payments = (
        silver_payments_df
        .join(
            changed_payment_ids,
            on="payment_id",
            how="inner"
        )
    )

    #getting latest records
    dim_customer_current = dim_customer.filter(col("is_current") == True)

    updated_fact_payments_records = build_fact_payments(affected_payments, dim_customer_current)

    final_dim_fact_payments = unchanged_fact_payments.unionByName(updated_fact_payments_records, allowMissingColumns=True)

    print(f'unchanged_fact_payments rows count :{unchanged_fact_payments.count()}')
    print(f'changed_payment_ids  count :{changed_payment_ids.count()}')
    print(f'affected_payments  count :{affected_payments.count()}')
    print(f'updated_fact_payments_records  count :{updated_fact_payments_records.count()}')
    print(f' final_dim_fact_payments count : {final_dim_fact_payments.count()}')

    return final_dim_fact_payments


