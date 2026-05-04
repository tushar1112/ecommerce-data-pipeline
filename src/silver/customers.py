import socketserver
import os

# empty object for unix features (error handle)
if os.name == 'nt':
    socketserver.UnixStreamServer = object
    socketserver.UnixDatagramServer = object


from pyspark.sql import Window
from pyspark.sql.functions import *
#------------------------------------ code ----------------------------------------------------

def transform_customers(df):

    # 1.filtering out invalid customer ids
    df = df.withColumn(
        'Drop_reason',
        when(col("customer_id").isNull() | (~col("customer_id").startswith("CUST")) |
                   (length(col("customer_id"))!=10), 'Invalid customer ID')
    )

    # 1.Bad record based on duplicate customer_id
    window = Window.partitionBy('customer_id').orderBy('first_signup_date')
    df = df.withColumn(
        'ranking_customer_id',
        row_number().over(window)
        ).withColumn(
        'Drop_reason',
         when(col('Drop_reason').isNotNull(),col('Drop_reason'))\
        .when(col('ranking_customer_id')>1,'Duplicate_customer_ID')\
        .otherwise(lit(None)))

    # 2.replacing null gender with 'prefer not to say'
    df = df.fillna({"gender": 'Prefer not to say'})  # simple way

    # 3.invalid email types
    email_regex = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    df = df.withColumn(
        'Drop_reason',
         when(col('Drop_reason').isNotNull(),col('Drop_reason'))\
        .when(col("email").isNull() & (~col("email").rlike(email_regex)),'Null or Invalid Email')\
        .otherwise(lit(None))
    )

    #4. duplicate email Ids
    window = Window.partitionBy('email').orderBy('first_signup_date')
    df = df.withColumn(
        'ranking_email',
        row_number().over(window)
    ).withColumn('Drop_reason',
                  when(col('Drop_reason').isNotNull(),col('Drop_reason'))\
                 .when(col('ranking_email') > 1, 'Duplicate_email_ID')\
                 .otherwise(lit(None)))

    #5.dropping invalid phone numbers as per TRAI
    india_mobile_regex = r"^(91)?[6-9]\d{9}$"
    df = df.withColumn(
        'Drop_reason',
         when(col('Drop_reason').isNotNull(),col('Drop_reason'))
        .when(~col('phone').rlike(india_mobile_regex),'Invalid Phone Number')
        .otherwise(lit(None))
    )

    #6. dropping invalid DOB ( invalid format , age less than 18, DOB not greater than current date)
    df = df.withColumn('DOB', try_to_date(col("DOB"), 'dd-MM-yyyy'))\
            .withColumn('age_in_months', months_between(current_date(), col('DOB'))) \
            .withColumn('age_in_years', col('age_in_months') / 12)
    df = df.withColumn(
        'Drop_reason',
         when(col('Drop_reason').isNotNull(),col('Drop_reason'))\
        .when(col('DOB').isNull(),'Invalid DOB')\
        .when(col('age_in_years') < 18, 'Age less than 18')\
        .when((col('DOB') > current_date()),'Invalid DOB')\
        .otherwise(lit(None))
    )

    #7 dropping invalid signup dates
    df = df.withColumn('first_signup_date', try_to_date(col("first_signup_date"), 'dd-MM-yyyy'))

    df = df.withColumn(
        'Drop_reason',
        when(col('Drop_reason').isNotNull(), col('Drop_reason'))\
        .when((col('first_signup_date') > current_date()) | (col('first_signup_date') < col('DOB')),'Invalid First Signup Date')\
        .otherwise(lit(None))
    )

    df = df.drop('ranking_customer_id','ranking_email','age_in_months','age_in_years')
    bad_records = df.filter(col('Drop_reason').isNotNull())
    df = df.filter(col('Drop_reason').isNull()).drop('Drop_reason')

    return df,bad_records







