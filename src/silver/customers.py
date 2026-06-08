from pyspark.sql import Window
from pyspark.sql.functions import *
#------------------------------------ code ----------------------------------------------------

def transform_customers(df):

    # defining schema and fixing data types
    column_type_mapping = {
        "customer_id" : "string",
        "first_name" : "string",
        "last_name" : "string",
        "gender" : "string",
        "dob": "date",
        "email" : "string",
        "phone" : "string",
        "alternate_phone": "string",
        "address" : "string",
        "city" : "string",
        "state" : "string",
        "postal_code" : "string",
        "country" : "string",
        "customer_status": "string",
        "signup_channel": "string",
        "marketing_opt_in":"boolean",
        "first_signup_date":"timestamp",
        "last_login_date":"timestamp",
        "last_purchase_date":"timestamp",
        "created_at":"timestamp",
        "updated_at":"timestamp",
        "deleted_flag":"boolean",
        "source_system":"string",
        "record_version":"int",
        "source_table": "string",
        "ingestion_timestamp": "timestamp",
        "ingestion_date": "date",
        "batch_id": "string",
        "operation_type": "string"
    }
    for column_name, data_type in column_type_mapping.items():
        df = df.withColumn(column_name,col(column_name).cast(data_type))

    # Initialize rejection reason
    df = df.withColumn("drop_reason", lit(None))
    
    # 0.filtering out invalid customer ids
    df = df.withColumn(
        'drop_reason',
        when(col("customer_id").isNull() | (~col("customer_id").startswith("CUST")) |
                   (length(col("customer_id"))!=10), lit('Invalid customer ID'))
        .otherwise(col("drop_reason"))
    )

    # 1.Bad record based on duplicate customer_id
    cust_window = Window.partitionBy('customer_id').orderBy(col('updated_at').desc())
    df = df.withColumn(
        'customer_rank',
        row_number().over(cust_window)
        ).withColumn(
        'drop_reason',
         when(col('drop_reason').isNotNull(),col('drop_reason'))\
        .when(col('customer_rank')>1,lit('Duplicate_customer_ID'))\
        .otherwise(col("drop_reason"))
        )

    # 2.replacing null gender with 'prefer not to say'
    df = df.withColumn(
        "gender",
        when(col("gender").isin("male", "m", "Male","M"), lit("Male"))
        .when(col("gender").isin("female", "f","Female","F"), lit("Female"))
        .when(col("gender").isin("other", "others"), lit("Other"))
        .otherwise(lit("Unknown"))
    )


    # 3.invalid email,phone
    email_regex = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    df = df.withColumn(
        'email_valid_flag',
         when((col("email").isNotNull()) & (col("email").rlike(email_regex)),lit(True))\
        .otherwise(lit(False))
        )

    #4. duplicate email Ids
    email_window = Window.partitionBy('email')
    df = df.withColumn(
        'email_count',
         when(col("email").isNotNull(), count("email").over(email_window))
         .otherwise(lit(0))
        )
    df = df.withColumn(
        "email_duplicate_flag",
        when(col('email_count') > 1, lit(True)).otherwise(lit(False))
    )

    #5.dropping invalid phone numbers as per TRAI
    india_mobile_regex = r"^(91)?[6-9]\d{9}$"
    df = df.withColumn(
        'phone_valid_flag',
        when(col('phone').rlike(india_mobile_regex),lit(True))
        .otherwise(lit(False))
    )

    #6. dropping invalid DOB ( invalid format , age less than 18, DOB not greater than current date)
    df = df.withColumn(
        'drop_reason',
        when(col('dob').isNull(),'Null DOB')\
        .when((col('dob') > current_date()),'Future DOB')\
        .otherwise(col('drop_reason'))
    )

    #7 dropping invalid signup dates
    df = df.withColumn(
        'drop_reason',
        when((col('first_signup_date') > current_date()) ,lit('Future signup Date'))
        .when((col('first_signup_date') < col('dob')), lit('Signup before dob'))
        .otherwise(col('drop_reason'))
    )
    #8 data quality status
    df = df.withColumn(
        "data_quality_status",
        when(
            (col("email_valid_flag") == False)
            | (col("phone_valid_flag") == False)
            | (col("email_duplicate_flag") == True),
            lit("WARNING")
        ).otherwise(lit("VALID"))
    )
    # if deleted_flag is true, make customer status discontinued
    df = df.withColumn(
        "customer_status",
        when(col("deleted_flag") == True, lit("DISCONTINUED"))
        .otherwise(col("customer_status"))
    )

    df = df.withColumn("silver_processed_timestamp", current_timestamp())

    bad_records = (
        df.filter(col("drop_reason").isNotNull())
        .withColumnRenamed("drop_reason", "rejection_reason")
        .withColumn("quarantine_timestamp", current_timestamp())
        .drop("customer_rank", "email_count")
    )

    clean_df = (
        df.filter(col("drop_reason").isNull())
        .drop("drop_reason", "customer_rank", "email_count")
    )

    return clean_df,bad_records







