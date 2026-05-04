
from pyspark.sql import Window
from pyspark.sql.functions import *

def transform_sellers(df):

    # 1.Bad record based on invalid seller id
    df = df.withColumn(
        'Drop_reason',
        when(col("seller_id").isNull() | (~col("seller_id").startswith("SELL")) |
             (length(col("seller_id")) != 9), 'Invalid seller ID')
        .otherwise(lit(None))
    )

    # 1.Bad record based on duplicate seller id
    window = Window.partitionBy('seller_id').orderBy(col('reg_date').desc())
    df = df.withColumn(
        'ranking_seller_id',
        row_number().over(window)
    ).withColumn(
        'Drop_reason',
        when(col('Drop_reason').isNotNull(), col('Drop_reason')) \
        .when(col('ranking_seller_id') > 1, 'Duplicate_seller_ID')
    )

    # fixing categories...
    my_list = ['Electronics', 'Furniture', 'clothing', 'beauty', 'sports', 'home']
    ref_list = [i.lower() for i in my_list]

    df = df.withColumn(
        "Drop_reason",
        when(col("Drop_reason").isNotNull(), col("Drop_reason"))
        .when(
            exists(
                split(col("sell_product_category"), ","),
                lambda x: ~(lower(trim(x)).isin(ref_list))
            ),
            "invalid category"
        )
    )

    #fixing future dates in reg_date
    df = (df.withColumn('reg_date',try_to_date(col('reg_date'),'dd-MM-yyyy'))
        .withColumn(
        'Drop_reason',
        when(col('Drop_reason').isNotNull(),col('Drop_reason'))
        .when(col('reg_date') > current_date(),'Invalid reg_date')
    ))
    df = df.withColumn(
        'active_status',
        when(~(lower(col('active_status')).isin(['active','inactive'])),lit('inactive'))
        .otherwise(col('active_status'))
    )

    df = df.drop('ranking_seller_id')

    bad_records = df.filter(col('Drop_reason').isNotNull())
    df = df.filter(col('Drop_reason').isNull()).drop('Drop_reason')

    return df, bad_records



