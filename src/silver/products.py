from pyspark.sql.functions import *

def transform_products(df):

    df = df.withColumn(
        'Drop_reason',
         when(col('product_id').isNull() | (trim(col('product_id')) == ""), 'Missing product_id')
        .when(~col("product_id").startswith("PROD") | (length(col("product_id")) != 10), 'Invalid product_id')
        .otherwise(None)
    )

    _reg_ex = r'[^0-9.]'
    df = df.withColumn(
        'Drop_reason',
        when(col('Drop_reason').isNotNull(),col('Drop_reason'))\
        .when(col('price').isNull(),'Missing price')\
        .when(col('price').rlike(_reg_ex), 'Invalid price')
        .when(col('price').cast('float') <= 0 ,'Invalid price')
    )


    df = df.withColumn('product_name', trim(col('product_name')))

    # giving camel case to brand name
    df = df.withColumn('brand', initcap(col('brand')))

    # fixing invalid categories
    list_of_categories = ['Electronics', 'Furniture', 'clothing', 'beauty', 'sports', 'home']
    df = df.withColumn('category', when(col('category').isin(list_of_categories), col('category')) \
                       .otherwise(lit('Other'))
                       )

    # fixing colors
    colors = ["Red", "Blue", "Black", "White", "Green", "Yellow"]
    df = df.withColumn(
        'color',
        when(col('color').isin(colors), col('color'))
        .otherwise(lit('other'))
    )

    bad_records = df.filter(col('Drop_reason').isNotNull())  # bad record
    df = df.filter(col('Drop_reason').isNull()).drop('Drop_reason')  # good record

    return df, bad_records



