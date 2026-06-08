from pyspark.sql.functions import *

def build_dim_date(spark, start_date, end_date):
    dim_date = (
        spark.sql(
        f"""
        select explode(
            sequence(
            to_date('{start_date}'), to_date('{end_date}'), interval 1 day
            )
        ) as full_date
        """
    )
    .withColumn('date_key',date_format(col('full_date'),'yyyyMMdd').cast('int'))
    .withColumn('year', year(col('full_date')))
    .withColumn('quarter', quarter(col('full_date')))
    .withColumn('month', month(col('full_date')))
    .withColumn('month_name', date_format(col('full_date'),'MMMM'))
    .withColumn('week_of_year', weekofyear(col('full_date')))
    .withColumn('day_of_month', dayofmonth(col('full_date')))
    .withColumn('day_of_week', dayofweek(col('full_date')))
    .withColumn(
            'is_weekend',
            when(date_format(col('full_date'),'E').isin('Sat','Sun'), True)
            .otherwise(False)
        )
    )

    dim_date = dim_date.select(
        "date_key",
        "full_date",
        "year",
        "quarter",
        "month",
        "month_name",
        "week_of_year",
        "day_of_month",
        "day_of_week",
        "is_weekend"
    )

    return dim_date
