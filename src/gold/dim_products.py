from pyspark.sql.functions import *
from pyspark.sql.window import Window


def get_categories_and_supplier_details(products_df, category_df, supplier_df):
    dim_products = (products_df.alias('p').filter(
        col('p.deleted_flag') == False
    ).join(
        category_df.alias('c'),
        col('c.category_id') == col('p.category_id'),
        'left'
    ).join(
        supplier_df.alias('s'),
        col('s.supplier_id') == col('p.supplier_id'),
        'left'
    ).select(
        col("p.product_id"),
        col("p.product_name"),
        col("p.brand"),
        col("p.sku"),

        col("p.category_id"),
        col("c.category_name"),

        col("p.supplier_id"),
        col("s.supplier_name"),
        col("s.supplier_type"),

        col("p.mrp"),
        col("p.selling_price"),
        col("p.cost_price"),
        col("p.product_rating"),
        col("p.total_reviews"),
        col("p.product_status"),
        col("p.is_returnable"),
        col("p.launch_date")
    ).dropDuplicates(['product_id'])
                    )
    return dim_products

def select_dim_products_attributes(dim_products):
    dim_products = dim_products.select(
        'product_sk',
        "product_id",
        "product_name",
        "brand",
        "sku",
        "category_id",
        "category_name",
        "supplier_id",
        "supplier_name",
        "supplier_type",
        "mrp",
        "selling_price",
        "cost_price",
        "product_rating",
        "total_reviews",
        "product_status",
        "is_returnable",
        "launch_date",
        "effective_start_date",
        "effective_end_date",
        "is_current"
    )
    return dim_products

def build_dim_products(products_df, category_df, supplier_df):

    # joining tables to get the required details
    dim_products = get_categories_and_supplier_details(products_df, category_df, supplier_df)
    
    # providing surrogate key
    dim_products = dim_products.withColumn(
        "product_sk",
        monotonically_increasing_id()
    )

    dim_products = (
        dim_products.withColumn('effective_start_date',current_date())
        .withColumn('effective_end_date',to_date(lit("9999-12-31")))
        .withColumn('is_current',lit(True))
    )

    # final ordering for columns
    return select_dim_products_attributes(dim_products)


def build_dim_products_inc(silver_products_df,gold_dim_product, category_df, supplier_df):
    # 1. active product in dim product(because we don't care about deleted for any changes)
    active_dim_products = gold_dim_product.filter(
        col('is_current') == True
    )
    # print(f' count of active_dim_products : {active_dim_products.count()}')

    # 2. deleted products from silver, mark it delete in gold, to make it consistent
    deleted_product_ids = silver_products_df.filter(
        col("deleted_flag") == True
    ).select("product_id").distinct()

    expired_deleted_rows = (
        gold_dim_product
        .join(
            deleted_product_ids,
            on="product_id",
            how="inner"
        )
        .withColumn("effective_end_date", current_date())
        .withColumn("product_status", lit("DISCONTINUED"))
        .withColumn("is_current", lit(False))
    )
    # print(f' count of deleted_product_ids : {deleted_product_ids.count()}')
    # print(f' count of expired_deleted_rows : {expired_deleted_rows.count()}')

    # Remove deleted products from active gold dim_product
    active_gold_after_delete_df = (
        active_dim_products
        .join(
            deleted_product_ids,
            on="product_id",
            how="left_anti"
        )
    )

    # Only non-deleted Silver records are used for insert/update logic
    active_silver_df = silver_products_df.filter(
        col("deleted_flag") == False
    )

    # -----------------------------------------------------
    # 3. Compare active Silver with active Gold, this gives all records from silver except inserts
    # -----------------------------------------------------
    comparison_df = (
        active_silver_df.alias("s")
        .join(
            active_gold_after_delete_df.alias("g"),
            on="product_id",
            how="inner"
        )
    )

    # 4. Detect SCD2 changes
    # These columns create history
    # -----------------------------------------------------
    scd2_product_ids = (
        comparison_df
        .filter(
            (coalesce(col("s.category_id"), lit("")) != coalesce(col("g.category_id"), lit(""))) |
            (coalesce(col("s.supplier_id"), lit("")) != coalesce(col("g.supplier_id"), lit(""))) |
            (coalesce(col("s.mrp"), lit("")) != coalesce(col("g.mrp"), lit(""))) |
            (coalesce(col("s.selling_price"), lit("")) != coalesce(col("g.selling_price"), lit(""))) |
            (coalesce(col("s.cost_price"), lit("")) != coalesce(col("g.cost_price"), lit(""))) |
            (coalesce(col("s.product_status"), lit("")) != coalesce(col("g.product_status"), lit("")))
        )
        .select("product_id")
        .distinct()
    )

    # 5. Expire old rows from gold_dim_product for SCD2 changes
    # -----------------------------------------------------
    expired_scd2_rows = (
        active_gold_after_delete_df
        .join(
            scd2_product_ids,
            on="product_id",
            how="inner"
        )
        .withColumn("effective_end_date", current_date())
        .withColumn("is_current", lit(False))
    )

    # 6. New current rows for SCD2 changed products
    # -----------------------------------------------------
    scd2_new_source = (
        active_silver_df
        .join(
            scd2_product_ids,
            on="product_id",
            how="inner"
        )
    )

    # 7. Brand new products
    # -----------------------------------------------------
    new_product_source = (
        active_silver_df
        .join(
            active_gold_after_delete_df.select("product_id"),
            on="product_id",
            how="left_anti"
        )
    )

    # 8. SCD1 overwrite rows
    # products not SCD2-changed, not deleted.
    # Rebuild current row from Silver but preserve SK/effective dates.
    # -----------------------------------------------------
    scd1_overwrite_rows = (
        active_gold_after_delete_df.alias("g")
        .join(
            scd2_product_ids,
            on="product_id",
            how="left_anti"
        )
        .join(
            active_silver_df.alias("s"),
            on="product_id",
            how="inner"
        )
        .select(
            col("g.product_sk"),
            col("g.product_id"),
            col("s.product_name"),
            col("g.brand"),
            col("g.sku"),
            col("g.category_id"),
            col("g.supplier_id"),
            col("g.mrp"),
            col("g.selling_price"),
            col("g.cost_price"),
            col("s.product_weight_grams"),
            col("s.product_rating"),
            col("s.total_reviews"),
            col("g.product_status"),
            col("s.is_returnable"),
            col("g.launch_date"),
            col("s.operation_type"),
            col("s.deleted_flag"),
            col("g.effective_start_date"),
            col("g.effective_end_date"),
            col("g.is_current")
        )
    )

    # 9. Historical rows already expired earlier
    # -----------------------------------------------------
    historical_rows = gold_dim_product.filter(
        col("is_current") == False
    )

    # -----------------------------------------------------
    # 10. Rows needing new surrogate keys
    # SCD2 new versions + brand new products
    # -----------------------------------------------------
    rows_needing_new_sk = (
        scd2_new_source
        .unionByName(new_product_source, allowMissingColumns=True)
    )

    max_sk = gold_dim_product.agg(max("product_sk")).collect()[0][0]
    max_sk = 0 if max_sk is None else int(max_sk)

    sk_window = Window.orderBy("product_id")

    rows_needing_new_sk = (
        rows_needing_new_sk
        .withColumn("rn", row_number().over(sk_window))
        .withColumn("product_sk", col("rn") + lit(max_sk))
        .drop("rn")
        .withColumn("effective_start_date", current_date())
        .withColumn("effective_end_date", to_date(lit("9999-12-31")))
        .withColumn("is_current", lit(True))
    )

    #getting category and supplier details for scd1_overwrite_rows, new_current_rows
    scd1_overwrite_rows = scd1_overwrite_rows.alias('p').filter(
        col('p.deleted_flag') == False
    ).join(
        category_df.alias('c'),
        col('c.category_id') == col('p.category_id'),
        'left'
    ).join(
        supplier_df.alias('s'),
        col('s.supplier_id') == col('p.supplier_id'),
        'left'
    ).select(
        col("p.product_sk"),
        col("p.product_id"),
        col("p.product_name"),
        col("p.brand"),
        col("p.sku"),

        col("p.category_id"),
        col("c.category_name"),

        col("p.supplier_id"),
        col("s.supplier_name"),
        col("s.supplier_type"),

        col("p.mrp"),
        col("p.selling_price"),
        col("p.cost_price"),
        col("p.product_rating"),
        col("p.total_reviews"),
        col("p.product_status"),
        col("p.is_returnable"),
        col("p.launch_date"),
        col("p.effective_start_date"),
        col("p.effective_end_date"),
        col("p.is_current")
    )
    new_current_rows = rows_needing_new_sk.alias('p').filter(
        col('p.deleted_flag') == False
    ).join(
        category_df.alias('c'),
        col('c.category_id') == col('p.category_id'),
        'left'
    ).join(
        supplier_df.alias('s'),
        col('s.supplier_id') == col('p.supplier_id'),
        'left'
    ).select(
        col("p.product_sk"),
        col("p.product_id"),
        col("p.product_name"),
        col("p.brand"),
        col("p.sku"),

        col("p.category_id"),
        col("c.category_name"),

        col("p.supplier_id"),
        col("s.supplier_name"),
        col("s.supplier_type"),

        col("p.mrp"),
        col("p.selling_price"),
        col("p.cost_price"),
        col("p.product_rating"),
        col("p.total_reviews"),
        col("p.product_status"),
        col("p.is_returnable"),
        col("p.launch_date"),
        col("p.effective_start_date"),
        col("p.effective_end_date"),
        col("p.is_current")
    )

    # print(historical_rows.columns)
    # print(expired_deleted_rows.columns)
    # print(expired_scd2_rows.columns)
    # print(scd1_overwrite_rows.columns)
    # print(new_current_rows.columns)
    # -----------------------------------------------------
    # 11. Final dim_product
    # -----------------------------------------------------
    final_dim_product = (
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
    print(f'count of final_dim_product : {final_dim_product.count()}')

    return final_dim_product