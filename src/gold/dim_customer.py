from pyspark.sql.functions import *
from pyspark.sql import Window

from src.common.watermark import get_last_processed_batch, update_watermark


def select_dim_customer_attributes(dim_customer):
    
    dim_customer = dim_customer.withColumn(
        "customer_name", concat_ws(" ",col('first_name'), col('last_name'))
    ).select(
        "customer_sk",
        "customer_id",
        "customer_name",
        "gender",
        "dob",
        "city",
        "state",
        "postal_code",
        "country",
        "customer_status",
        "signup_channel",
        "marketing_opt_in",
        "first_signup_date",
        "last_login_date",
        "last_purchase_date",
        "effective_start_date",
        "effective_end_date",
        "is_current"
    )
    return dim_customer


def build_dim_customer(customer_df):
    dim_customer = (
        customer_df.filter( col('deleted_flag') == False)
        .withColumn( 'customer_name', concat_ws(" ",col('first_name'), col('last_name')))
        .dropDuplicates(['customer_id'])
        .withColumn("customer_sk", monotonically_increasing_id())
        .withColumn('effective_start_date',current_date())
        .withColumn('effective_end_date',to_date(lit("9999-12-31")))
        .withColumn('is_current',lit(True))
    )

    return select_dim_customer_attributes(dim_customer)


def build_dim_customers_inc(silver_customer_df,gold_dim_customer):


    """
    silver customer has 3 types of records that we need to handle and manage in gold customer
    1. records that are deleted or delete flag = true in silver customer. [action] -> expire them in gold directly
    2. records with updates. these updates are classified into SCD1 and SCD2
            - SCD1 changes will directly overwrite gold customer(no history tracked)
            - SCD2 changed 1st expire rows, then add new updated rows (track history)
    3. records with new inserts. These are directly inserted into gold customer

    To do this, we will form dim_customer in chunks and then merge them at last.
        unchanged row + expired as deleted + (expired by SCD2 + new_row by SCD2) + rows by SCD1 + new_inserts
    """

    # 1. active customer in dim customer(because we don't care about deleted for any changes)
    active_dim_customers = gold_dim_customer.filter(
        col('is_current') == True
    )

    # print(f' count of active_dim_customers : {active_dim_customers.count()}')

    #2. deleted customers from silver, mark it delete in gold, to make it consistent
    deleted_customer_ids = silver_customer_df.filter(
        col("deleted_flag") == True
        ).select("customer_id").distinct()

    expired_deleted_rows = (
        gold_dim_customer
        .join(
            deleted_customer_ids,
            on="customer_id",
            how="inner"
        )
        .withColumn("effective_end_date", current_date())
        .withColumn("is_current", lit(False))
    )
    # print(f' count of deleted_customer_ids : {deleted_customer_ids.count()}')
    # print(f' count of expired_deleted_rows : {expired_deleted_rows.count()}')

    # Remove deleted customers from active gold dim_customer
    active_gold_after_delete_df = (
        active_dim_customers
        .join(
            deleted_customer_ids,
            on="customer_id",
            how="left_anti"
        )
    )

    # Only non-deleted Silver records are used for insert/update logic
    active_silver_df = silver_customer_df.filter(
        col("deleted_flag") == False
    )

    # -----------------------------------------------------
    # 3. Compare active Silver with active Gold, this gives all records from silver except inserts
    # -----------------------------------------------------
    comparison_df = (
        active_silver_df.alias("s")
        .join(
            active_gold_after_delete_df.alias("g"),
            on="customer_id",
            how="inner"
        )
    )

    # 4. Detect SCD2 changes
    # These columns create history
    # -----------------------------------------------------
    scd2_customer_ids = (
        comparison_df
        .filter(
            (coalesce(col("s.city"), lit("")) != coalesce(col("g.city"), lit(""))) |
            (coalesce(col("s.state"), lit("")) != coalesce(col("g.state"), lit(""))) |
            (coalesce(col("s.customer_status"), lit("")) != coalesce(col("g.customer_status"), lit(""))) |
            (
                    coalesce(col("s.marketing_opt_in").cast("string"), lit("")) !=
                    coalesce(col("g.marketing_opt_in").cast("string"), lit(""))
            )
        )
        .select("customer_id")
        .distinct()
    )

    # 5. Expire old rows from gold_dim_customer for SCD2 changes
    # -----------------------------------------------------
    expired_scd2_rows = (
        active_gold_after_delete_df
        .join(
            scd2_customer_ids,
            on="customer_id",
            how="inner"
        )
        .withColumn("effective_end_date", current_date())
        .withColumn("is_current", lit(False))
    )

    # 6. New current rows for SCD2 changed customers
    # -----------------------------------------------------
    scd2_new_source = (
        active_silver_df
        .join(
            scd2_customer_ids,
            on="customer_id",
            how="inner"
        )
    )

    # 7. Brand new customers
    # -----------------------------------------------------
    new_customer_source = (
        active_silver_df
        .join(
            active_gold_after_delete_df.select("customer_id"),
            on="customer_id",
            how="left_anti"
        )
    )

    # 8. SCD1 overwrite rows
    # Customers not SCD2-changed, not deleted.
    # Rebuild current row from Silver but preserve SK/effective dates.
    # -----------------------------------------------------
    scd1_overwrite_rows = (
        active_gold_after_delete_df.alias("g")
        .join(
            scd2_customer_ids,
            on="customer_id",
            how="left_anti"
        )
        .join(
            active_silver_df.alias("s"),
            on="customer_id",
            how="inner"
        )
        .select(
            col("g.customer_sk"),
            col("s.customer_id"),
            concat_ws(" ", col("s.first_name"), col("s.last_name")).alias("customer_name"),
            col("s.gender"),
            col("s.dob"),
            col("s.city"),
            col("s.state"),
            col("s.postal_code"),
            col("s.country"),
            col("s.customer_status"),
            col("s.signup_channel"),
            col("s.marketing_opt_in"),
            col("s.first_signup_date"),
            col("s.last_login_date"),
            col("s.last_purchase_date"),
            col("g.effective_start_date"),
            col("g.effective_end_date"),
            col("g.is_current")
        )
    )

    # 9. Historical rows already expired earlier
    # -----------------------------------------------------
    historical_rows = gold_dim_customer.filter(
        col("is_current") == False
    )

    # -----------------------------------------------------
    # 10. Rows needing new surrogate keys
    # SCD2 new versions + brand new customers
    # -----------------------------------------------------
    rows_needing_new_sk = (
        scd2_new_source
        .unionByName(new_customer_source, allowMissingColumns=True)
    )

    max_sk = gold_dim_customer.agg(max("customer_sk")).collect()[0][0]
    max_sk = 0 if max_sk is None else int(max_sk)

    sk_window = Window.orderBy("customer_id")

    rows_needing_new_sk = (
        rows_needing_new_sk
        .withColumn("rn", row_number().over(sk_window))
        .withColumn("customer_sk", col("rn") + lit(max_sk))
        .drop("rn")
        .withColumn("effective_start_date", current_date())
        .withColumn("effective_end_date", to_date(lit("9999-12-31")))
        .withColumn("is_current", lit(True))
    )

    new_current_rows = select_dim_customer_attributes(rows_needing_new_sk)

    # -----------------------------------------------------
    # 11. Final dim_customer
    # -----------------------------------------------------
    final_dim_customer = (
        historical_rows
        .unionByName(expired_deleted_rows, allowMissingColumns=True)
        .unionByName(expired_scd2_rows, allowMissingColumns=True)
        .unionByName(scd1_overwrite_rows, allowMissingColumns=True)
        .unionByName(new_current_rows, allowMissingColumns=True)
    )

    print(f'historical rows count :{historical_rows.count()}')
    print(f'expired_deleted_rows  count :{expired_deleted_rows.count()}')
    print(f'expired_scd2_rows  count :{expired_scd2_rows.count()}')
    print(f'scd1_overwrite_rows  count :{scd1_overwrite_rows.count()}')
    print(f'new_current_rows  count :{new_current_rows.count()}')
    print(f'count of final_dim_customer : {final_dim_customer.count()}')

    return final_dim_customer
    # return gold_dim_customer
